package io.github.moalrubaie.refunds.demo;

import io.github.moalrubaie.refunds.RefundPayerApplication;
import io.github.moalrubaie.refunds.Stats;
import java.io.OutputStream;
import java.io.PrintStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

/**
 * Runs the experiment: publish one stream of refunds, then for each sink start the real service
 * against it, let it catch up (round 1), rewind its consumer group to the start of the topic and let
 * it catch up again (round 2, a full replay), and count what was paid.
 */
public final class DemoRunner {

    static final String TOPIC = "refunds.approved";
    static final String DEAD_LETTER_TOPIC = "refunds.approved.DLT";
    static final int PARTITIONS = 3;
    static final List<String> ALL_SINKS = List.of("naive", "check-then-record", "idempotent");

    /**
     * @param bootstrap a Kafka or Redpanda address, or null to start an in-process Kafka broker
     * @param jdbcUrl a Postgres URL, or null for a fresh in-memory H2 database per sink
     */
    public record Options(
            String bootstrap,
            String jdbcUrl,
            String jdbcUser,
            String jdbcPassword,
            int events,
            double resendRate,
            double crashRate,
            int poison,
            long seed,
            List<String> sinks) {

        public static Options defaults() {
            return new Options(null, null, null, null, 1000, 0.03, 0.05, 4, 7, ALL_SINKS);
        }

        public Options withBroker(String bootstrap) {
            return new Options(
                    bootstrap, jdbcUrl, jdbcUser, jdbcPassword, events, resendRate, crashRate, poison, seed, sinks);
        }

        public Options withDatabase(String url, String user, String password) {
            return new Options(bootstrap, url, user, password, events, resendRate, crashRate, poison, seed, sinks);
        }

        public Options withEvents(int events) {
            return new Options(
                    bootstrap, jdbcUrl, jdbcUser, jdbcPassword, events, resendRate, crashRate, poison, seed, sinks);
        }
    }

    /** What one sink did in one round. Delivery counts are for the round; ledger figures are totals. */
    public record Round(
            long deliveries,
            long crashes,
            long duplicatesSkipped,
            long deadLettered,
            long payouts,
            long paidCents,
            long doublePaid,
            long unpaid,
            long expectedCents) {

        public long overpaidCents() {
            return paidCents - expectedCents;
        }

        public boolean exact() {
            return paidCents == expectedCents && doublePaid == 0 && unpaid == 0;
        }
    }

    public record SinkResult(String sink, Round firstPass, Round replay) {}

    public record Result(String broker, String database, RefundStream.Published published, List<SinkResult> sinks) {

        public SinkResult sink(String name) {
            return sinks.stream().filter(s -> s.sink().equals(name)).findFirst().orElseThrow();
        }
    }

    private static final Duration CATCH_UP_TIMEOUT = Duration.ofMinutes(3);

    private DemoRunner() {}

    public static Result run(Options options, Consumer<String> progress) throws Exception {
        EmbeddedKafkaKraftBroker embedded = null;
        String bootstrap = options.bootstrap();
        if (bootstrap == null) {
            progress.accept("Starting an in-process Kafka broker...");
            embedded = new EmbeddedKafkaKraftBroker(1, PARTITIONS);
            startQuietly(embedded);
            bootstrap = embedded.getBrokersAsString();
        }
        try (Admin admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
            if (embedded == null) {
                resetTopicsAndGroups(admin, options.sinks()); // an earlier run may have left them behind
            }
            createTopics(admin);
            var published = RefundStream.publish(
                    bootstrap, TOPIC, options.events(), options.resendRate(), options.poison(), options.seed());
            progress.accept("Published %,d records to %s.".formatted(published.records(), TOPIC));

            var results = new ArrayList<SinkResult>();
            for (String sink : options.sinks()) {
                progress.accept("Running the " + sink + " sink...");
                results.add(runSink(sink, bootstrap, admin, options, published));
            }
            return new Result(
                    embedded != null ? "in-process Kafka" : bootstrap,
                    options.jdbcUrl() == null ? "H2 (in memory)" : options.jdbcUrl(),
                    published,
                    results);
        } finally {
            if (embedded != null) {
                embedded.destroy();
            }
        }
    }

    /** Kafka's storage formatter prints to stdout while the broker starts. Keep the demo output clean. */
    private static void startQuietly(EmbeddedKafkaKraftBroker broker) {
        PrintStream out = System.out;
        System.setOut(new PrintStream(OutputStream.nullOutputStream()));
        try {
            broker.afterPropertiesSet();
        } finally {
            System.setOut(out);
        }
    }

    private static SinkResult runSink(
            String sink, String bootstrap, Admin admin, Options options, RefundStream.Published published)
            throws Exception {
        String group = "refund-payer-" + sink;
        Map<String, Object> props = new HashMap<>();
        props.put("spring.kafka.bootstrap-servers", bootstrap);
        props.put("spring.kafka.consumer.group-id", group);
        props.put("spring.kafka.listener.auto-startup", "false");
        props.put("app.topic", TOPIC);
        props.put("app.dead-letter-topic", DEAD_LETTER_TOPIC);
        props.put("app.partitions", PARTITIONS);
        props.put("app.sink", sink);
        props.put("app.retry-backoff", "20ms");
        props.put("app.chaos.crash-after-effect-rate", options.crashRate());
        props.put("app.chaos.seed", options.seed());
        if (options.jdbcUrl() == null) {
            props.put(
                    "spring.datasource.url",
                    "jdbc:h2:mem:" + sink + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        } else {
            props.put("spring.datasource.url", options.jdbcUrl());
            props.put("spring.datasource.username", options.jdbcUser());
            props.put("spring.datasource.password", options.jdbcPassword());
        }

        // As command-line arguments, so they win over application.yml.
        String[] args = props.entrySet().stream()
                .map(e -> "--" + e.getKey() + "=" + e.getValue())
                .toArray(String[]::new);
        try (var ctx = new SpringApplicationBuilder(RefundPayerApplication.class)
                .web(WebApplicationType.NONE)
                .logStartupInfo(false)
                .run(args)) {
            var jdbc = ctx.getBean(JdbcClient.class);
            var stats = ctx.getBean(Stats.class);
            MessageListenerContainer container =
                    ctx.getBean(KafkaListenerEndpointRegistry.class).getListenerContainer("refund-payer");
            jdbc.sql("TRUNCATE TABLE payouts").update();
            jdbc.sql("TRUNCATE TABLE processed_events").update();

            // Round 1: the stream as published, with resends, poison and crashes.
            container.start();
            awaitCaughtUp(admin, group);
            var afterFirst = stats.snapshot();
            var first = round(afterFirst, jdbc, published);

            // Round 2: rewind the group to offset 0 and deliver everything again.
            container.stop();
            awaitEmpty(admin, group);
            rewind(admin, group);
            container.start();
            awaitCaughtUp(admin, group);
            var replay = round(stats.snapshot().minus(afterFirst), jdbc, published);
            container.stop();
            return new SinkResult(sink, first, replay);
        }
    }

    private static Round round(Stats.Snapshot s, JdbcClient jdbc, RefundStream.Published published) {
        long payouts =
                jdbc.sql("SELECT COUNT(*) FROM payouts").query(Long.class).single();
        long paid = jdbc.sql("SELECT COALESCE(SUM(amount_cents), 0) FROM payouts")
                .query(Long.class)
                .single();
        long doublePaid = jdbc.sql(
                        "SELECT COUNT(*) FROM (SELECT event_id FROM payouts GROUP BY event_id HAVING COUNT(*) > 1) d")
                .query(Long.class)
                .single();
        long distinct = jdbc.sql("SELECT COUNT(DISTINCT event_id) FROM payouts")
                .query(Long.class)
                .single();
        return new Round(
                s.deliveries(),
                s.crashes(),
                s.duplicatesSkipped(),
                s.deadLettered(),
                payouts,
                paid,
                doublePaid,
                published.refunds() - distinct,
                published.expectedCents());
    }

    // --- Kafka admin helpers -------------------------------------------------------------------

    private static void resetTopicsAndGroups(Admin admin, List<String> sinks) throws Exception {
        var groups = sinks.stream().map(s -> "refund-payer-" + s).toList();
        try {
            admin.deleteConsumerGroups(groups).all().get();
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof GroupIdNotFoundException)) {
                throw e;
            }
        }
        try {
            admin.deleteTopics(List.of(TOPIC, DEAD_LETTER_TOPIC)).all().get();
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
                throw e;
            }
        }
    }

    private static void createTopics(Admin admin) throws Exception {
        var newTopics = List.of(TOPIC, DEAD_LETTER_TOPIC).stream()
                .map(t -> new NewTopic(t, PARTITIONS, (short) 1))
                .toList();
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (true) { // a just-deleted topic can take a moment to go away
            try {
                admin.createTopics(newTopics).all().get();
                return;
            } catch (ExecutionException e) {
                if (!(e.getCause() instanceof TopicExistsException) || System.nanoTime() > deadline) {
                    throw e;
                }
                Thread.sleep(250);
            }
        }
    }

    private static List<TopicPartition> partitions(Admin admin) throws Exception {
        return admin.describeTopics(List.of(TOPIC)).allTopicNames().get().get(TOPIC).partitions().stream()
                .map(p -> new TopicPartition(TOPIC, p.partition()))
                .toList();
    }

    /** Waits until the group has committed every partition up to its end offset. */
    private static void awaitCaughtUp(Admin admin, String group) throws Exception {
        var request = partitions(admin).stream().collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()));
        var ends = admin.listOffsets(request).all().get();
        long deadline = System.nanoTime() + CATCH_UP_TIMEOUT.toNanos();
        while (true) {
            Map<TopicPartition, OffsetAndMetadata> committed = admin.listConsumerGroupOffsets(group)
                    .partitionsToOffsetAndMetadata()
                    .get();
            boolean caughtUp = ends.entrySet().stream().allMatch(e -> {
                long end = e.getValue().offset();
                var c = committed.get(e.getKey());
                return end == 0 || (c != null && c.offset() >= end);
            });
            if (caughtUp) {
                return;
            }
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException(group + " did not catch up within " + CATCH_UP_TIMEOUT);
            }
            Thread.sleep(100);
        }
    }

    private static void awaitEmpty(Admin admin, String group) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (true) {
            var state = admin.describeConsumerGroups(List.of(group))
                    .all()
                    .get()
                    .get(group)
                    .groupState();
            if (state == GroupState.EMPTY || state == GroupState.DEAD) {
                return;
            }
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException(group + " still has members after stopping: " + state);
            }
            Thread.sleep(100);
        }
    }

    /** The same as {@code rpk group seek <group> --to start}. */
    private static void rewind(Admin admin, String group) throws Exception {
        var toStart = partitions(admin).stream().collect(Collectors.toMap(tp -> tp, tp -> new OffsetAndMetadata(0)));
        admin.alterConsumerGroupOffsets(group, toStart).all().get();
    }
}
