package io.github.moalrubaie.refunds.sink;

import io.github.moalrubaie.refunds.Chaos;
import io.github.moalrubaie.refunds.RefundApproved;
import io.github.moalrubaie.refunds.Stats;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

/**
 * The common first attempt at deduplication: check a processed-events table, pay, then record the
 * event ID. It stops resends and replays, but the check, the payment and the record are three
 * separate steps. A crash after the payment and before the record leaves a payment nobody knows
 * about, and the redelivered event pays again.
 */
@Component
@ConditionalOnProperty(name = "app.sink", havingValue = "check-then-record")
public class CheckThenRecordSink implements RefundSink {

    private final JdbcClient jdbc;
    private final Payouts payouts;
    private final Chaos chaos;
    private final Stats stats;

    public CheckThenRecordSink(JdbcClient jdbc, Payouts payouts, Chaos chaos, Stats stats) {
        this.jdbc = jdbc;
        this.payouts = payouts;
        this.chaos = chaos;
        this.stats = stats;
    }

    @Override
    public void pay(RefundApproved event) {
        boolean seen = jdbc.sql("SELECT COUNT(*) FROM processed_events WHERE event_id = ?")
                        .param(event.eventId())
                        .query(Long.class)
                        .single()
                > 0;
        if (seen) {
            stats.duplicateSkipped();
            return;
        }
        payouts.pay(event); // 1. the effect, committed on its own
        chaos.afterEffect(event.eventId()); // a crash here leaves a payment with no record
        jdbc.sql("INSERT INTO processed_events (event_id) VALUES (?) ON CONFLICT DO NOTHING") // 2. the record
                .param(event.eventId())
                .update();
    }
}
