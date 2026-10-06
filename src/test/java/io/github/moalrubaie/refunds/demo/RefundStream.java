package io.github.moalrubaie.refunds.demo;

import io.github.moalrubaie.refunds.RefundApproved;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.SplittableRandom;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import tools.jackson.databind.json.JsonMapper;

/**
 * The upstream system: publishes a seeded stream of approved refunds, keyed by order ID, with two
 * kinds of trouble mixed in.
 *
 * <ul>
 *   <li><b>Resends.</b> Some events are published a second time a little later, the way a producer
 *       does when it retries after a lost acknowledgement. Kafka's idempotent producer can't catch
 *       these: to Kafka they are two different messages that happen to carry the same event ID.
 *   <li><b>Poison.</b> A few records that can never be processed: half are not valid JSON, half
 *       have a negative amount.
 * </ul>
 */
final class RefundStream {

    /**
     * @param records records written to the topic
     * @param refunds distinct valid refunds, which is how many payments should happen
     * @param expectedCents what those refunds add up to
     * @param resends records that repeat an earlier event
     * @param poison records that can never be processed
     */
    record Published(int records, int refunds, long expectedCents, int resends, int poison) {}

    private record Pending(int due, String key, String value) {}

    private RefundStream() {}

    static Published publish(String bootstrap, String topic, int events, double resendRate, int poison, long seed) {
        var mapper = JsonMapper.builder().build();
        var rnd = new SplittableRandom(seed);
        var out = new ArrayList<ProducerRecord<String, String>>();
        var later = new PriorityQueue<Pending>((a, b) -> Integer.compare(a.due(), b.due()));
        long expectedCents = 0;
        int resends = 0;
        int poisonWritten = 0;

        for (int i = 1; i <= events; i++) {
            while (!later.isEmpty() && later.peek().due() <= i) {
                var p = later.poll();
                out.add(new ProducerRecord<>(topic, p.key(), p.value()));
            }
            if (poisonWritten < poison && i == (poisonWritten + 1) * events / (poison + 1)) {
                out.add(poisonRecord(topic, poisonWritten++, mapper));
            }
            var event = new RefundApproved(
                    "refund-%05d".formatted(i),
                    "order-%04d".formatted(rnd.nextInt(1, Math.max(2, events * 6 / 10))),
                    rnd.nextLong(500, 25_001)); // $5.00 to $250.00
            String json = mapper.writeValueAsString(event);
            out.add(new ProducerRecord<>(topic, event.orderId(), json));
            expectedCents += event.amountCents();
            if (rnd.nextDouble() < resendRate) {
                later.add(new Pending(i + 1 + rnd.nextInt(25), event.orderId(), json));
                resends++;
            }
        }
        while (!later.isEmpty()) {
            var p = later.poll();
            out.add(new ProducerRecord<>(topic, p.key(), p.value()));
        }
        while (poisonWritten < poison) {
            out.add(poisonRecord(topic, poisonWritten++, mapper));
        }

        send(bootstrap, out);
        return new Published(out.size(), events, expectedCents, resends, poisonWritten);
    }

    private static ProducerRecord<String, String> poisonRecord(String topic, int n, JsonMapper mapper) {
        String key = "order-bad-" + n;
        String value = n % 2 == 0
                ? "{\"eventId\": \"refund-bad-" + n + "\", \"orderId\": \"" + key + "\", \"amountCents\": " // cut off
                : mapper.writeValueAsString(new RefundApproved("refund-bad-" + n, key, -1_000));
        return new ProducerRecord<>(topic, key, value);
    }

    private static void send(String bootstrap, List<ProducerRecord<String, String>> records) {
        Map<String, Object> config = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                bootstrap,
                ProducerConfig.ACKS_CONFIG,
                "all",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
                true,
                ProducerConfig.LINGER_MS_CONFIG,
                5);
        try (var producer = new KafkaProducer<>(config, new StringSerializer(), new StringSerializer())) {
            records.forEach(producer::send);
            producer.flush();
        }
    }
}
