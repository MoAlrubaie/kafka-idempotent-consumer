package io.github.moalrubaie.refunds.sink;

import io.github.moalrubaie.refunds.AppProperties;
import io.github.moalrubaie.refunds.Chaos;
import io.github.moalrubaie.refunds.Stats;
import java.time.Duration;
import java.util.UUID;
import javax.sql.DataSource;
import org.h2.jdbcx.JdbcDataSource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

/** Builds sinks by hand, without Spring Boot or Kafka, against a database that has the app's schema. */
final class Sinks {

    final DataSource dataSource;
    final JdbcClient jdbc;
    final Stats stats = new Stats();

    private Sinks(DataSource dataSource) {
        this.dataSource = dataSource;
        this.jdbc = JdbcClient.create(dataSource);
        new ResourceDatabasePopulator(new ClassPathResource("schema.sql")).execute(dataSource);
        jdbc.sql("TRUNCATE TABLE payouts").update();
        jdbc.sql("TRUNCATE TABLE processed_events").update();
    }

    static Sinks onH2() {
        var ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:" + UUID.randomUUID() + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1");
        return new Sinks(ds);
    }

    static Sinks on(DataSource dataSource) {
        return new Sinks(dataSource);
    }

    /** A sink by its app.sink name. {@code crashRate} 1.0 crashes every event once, after its effect. */
    RefundSink sink(String name, double crashRate) {
        var chaos = new Chaos(
                new AppProperties("t", "t.DLT", 1, name, 3, Duration.ZERO, new AppProperties.Chaos(crashRate, 7)),
                stats);
        var payouts = new Payouts(jdbc);
        return switch (name) {
            case "naive" -> new NaiveSink(payouts, chaos);
            case "check-then-record" -> new CheckThenRecordSink(jdbc, payouts, chaos, stats);
            case "idempotent" ->
                new IdempotentSink(jdbc, payouts, chaos, stats, new DataSourceTransactionManager(dataSource));
            default -> throw new IllegalArgumentException(name);
        };
    }

    long payouts() {
        return jdbc.sql("SELECT COUNT(*) FROM payouts").query(Long.class).single();
    }

    long payoutsFor(String eventId) {
        return jdbc.sql("SELECT COUNT(*) FROM payouts WHERE event_id = ?")
                .param(eventId)
                .query(Long.class)
                .single();
    }

    long processed() {
        return jdbc.sql("SELECT COUNT(*) FROM processed_events")
                .query(Long.class)
                .single();
    }
}
