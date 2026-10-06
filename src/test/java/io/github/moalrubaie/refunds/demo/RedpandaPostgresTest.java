package io.github.moalrubaie.refunds.demo;

import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.redpanda.RedpandaContainer;

/** The same run against real Redpanda and Postgres containers. Skipped when Docker isn't available. */
@Testcontainers(disabledWithoutDocker = true)
class RedpandaPostgresTest {

    @Container
    static final RedpandaContainer REDPANDA = new RedpandaContainer("redpandadata/redpanda:v26.2.3");

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18.6-alpine");

    @Test
    void onlyTheIdempotentSinkPaysEveryRefundExactlyOnce() throws Exception {
        var options = DemoRunner.Options.defaults()
                .withEvents(400)
                .withBroker(REDPANDA.getBootstrapServers())
                .withDatabase(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());

        var result = DemoRunner.run(options, line -> {});

        DemoAssertions.assertExpectedOutcome(result);
    }
}
