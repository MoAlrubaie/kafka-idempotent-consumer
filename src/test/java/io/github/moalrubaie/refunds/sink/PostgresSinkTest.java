package io.github.moalrubaie.refunds.sink;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.moalrubaie.refunds.RefundApproved;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** Copies of one event racing on different consumers, against real Postgres. Needs Docker. */
@Testcontainers(disabledWithoutDocker = true)
class PostgresSinkTest {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18.6-alpine");

    @Test
    void racingCopiesOfAnEventPayOnce() throws Exception {
        var ds = new PGSimpleDataSource();
        ds.setUrl(POSTGRES.getJdbcUrl());
        ds.setUser(POSTGRES.getUsername());
        ds.setPassword(POSTGRES.getPassword());
        var db = Sinks.on(ds);
        var sink = db.sink("idempotent", 0);
        int events = 50;
        int copies = 4;

        try (var pool = Executors.newFixedThreadPool(copies)) {
            for (int i = 0; i < events; i++) {
                var event = new RefundApproved("refund-" + i, "order-" + i, 1_000);
                var start = new CountDownLatch(1);
                var results = new ArrayList<Future<?>>();
                for (int c = 0; c < copies; c++) {
                    results.add(pool.submit(() -> {
                        start.await();
                        sink.pay(event);
                        return null;
                    }));
                }
                start.countDown(); // release all copies at once
                for (var r : results) {
                    r.get(); // no copy may fail: the losers wait for the winner, then find its row
                }
            }
        }

        assertThat(db.payouts()).isEqualTo(events);
        assertThat(db.stats.snapshot().duplicatesSkipped()).isEqualTo((long) events * (copies - 1));
    }
}
