package io.github.moalrubaie.refunds.sink;

import io.github.moalrubaie.refunds.Chaos;
import io.github.moalrubaie.refunds.RefundApproved;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * Pays every delivery. The only bookkeeping is the offset commit, which the listener container
 * makes after this method returns. A crash before that commit, an upstream resend or a replay
 * means paying again.
 */
@Component
@ConditionalOnProperty(name = "app.sink", havingValue = "naive")
public class NaiveSink implements RefundSink {

    private final Payouts payouts;
    private final Chaos chaos;

    public NaiveSink(Payouts payouts, Chaos chaos) {
        this.payouts = payouts;
        this.chaos = chaos;
    }

    @Override
    public void pay(RefundApproved event) {
        payouts.pay(event);
        chaos.afterEffect(event.eventId()); // the offset is not committed yet
    }
}
