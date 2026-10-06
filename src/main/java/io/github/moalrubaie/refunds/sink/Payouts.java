package io.github.moalrubaie.refunds.sink;

import io.github.moalrubaie.refunds.RefundApproved;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

/**
 * The side effect: money leaving the business, one row per payment.
 *
 * <p>{@code payouts.event_id} is deliberately not unique. Like most real effects (a card refund, an
 * email), making the payment twice simply happens twice. Protecting against that is the sink's job.
 */
@Component
public class Payouts {

    private final JdbcClient jdbc;

    public Payouts(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public void pay(RefundApproved event) {
        jdbc.sql("INSERT INTO payouts (event_id, order_id, amount_cents) VALUES (?, ?, ?)")
                .params(event.eventId(), event.orderId(), event.amountCents())
                .update();
    }
}
