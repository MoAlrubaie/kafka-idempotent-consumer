package io.github.moalrubaie.refunds.sink;

import io.github.moalrubaie.refunds.RefundApproved;

/**
 * Pays one refund. The listener calls it once per delivery, and Kafka may deliver the same event
 * more than once. The three implementations differ only in what they do about that.
 */
public interface RefundSink {

    void pay(RefundApproved event);
}
