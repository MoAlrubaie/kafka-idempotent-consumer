package io.github.moalrubaie.refunds.sink;

import io.github.moalrubaie.refunds.Chaos;
import io.github.moalrubaie.refunds.RefundApproved;
import io.github.moalrubaie.refunds.Stats;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Records the event ID and makes the payment in one database transaction, and lets the primary key
 * on {@code processed_events} decide who goes first.
 *
 * <ul>
 *   <li>A duplicate's insert finds the existing row, changes nothing, and the sink returns.
 *   <li>Two copies racing on different consumers: one insert wins, the other waits for it and then
 *       finds the row.
 *   <li>A crash anywhere inside rolls back both rows, so the redelivery starts clean.
 * </ul>
 *
 * <p>This works because the effect lives in the same database as the record. For an effect in
 * another system, pass the event ID to that system as its idempotency key instead.
 */
@Component
@ConditionalOnProperty(name = "app.sink", havingValue = "idempotent", matchIfMissing = true)
public class IdempotentSink implements RefundSink {

    private final JdbcClient jdbc;
    private final Payouts payouts;
    private final Chaos chaos;
    private final Stats stats;
    private final TransactionTemplate transaction;

    public IdempotentSink(
            JdbcClient jdbc, Payouts payouts, Chaos chaos, Stats stats, PlatformTransactionManager transactions) {
        this.jdbc = jdbc;
        this.payouts = payouts;
        this.chaos = chaos;
        this.stats = stats;
        this.transaction = new TransactionTemplate(transactions);
    }

    @Override
    public void pay(RefundApproved event) {
        transaction.executeWithoutResult(
                tx -> { // all or nothing
                    int inserted = jdbc.sql("INSERT INTO processed_events (event_id) VALUES (?) ON CONFLICT DO NOTHING")
                            .param(event.eventId())
                            .update();
                    if (inserted == 0) {
                        stats.duplicateSkipped(); // seen before: do nothing
                        return;
                    }
                    payouts.pay(event);
                    chaos.afterEffect(event.eventId()); // a crash here rolls back the payment and the record
                });
    }
}
