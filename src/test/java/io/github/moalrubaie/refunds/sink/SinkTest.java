package io.github.moalrubaie.refunds.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.moalrubaie.refunds.Chaos.SimulatedCrashException;
import io.github.moalrubaie.refunds.RefundApproved;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** The three sinks against the two kinds of duplicate, without Kafka. */
class SinkTest {

    private static final List<RefundApproved> EVENTS = List.of(
            new RefundApproved("refund-1", "order-1", 12_000),
            new RefundApproved("refund-2", "order-2", 4_550),
            new RefundApproved("refund-3", "order-1", 999));

    @ParameterizedTest(name = "{0} pays {1} times when every event arrives twice")
    @CsvSource({"naive, 6", "check-then-record, 3", "idempotent, 3"})
    void everyEventDeliveredTwice(String name, long expectedPayouts) {
        var db = Sinks.onH2();
        var sink = db.sink(name, 0);

        for (int round = 0; round < 2; round++) {
            EVENTS.forEach(sink::pay);
        }

        assertThat(db.payouts()).isEqualTo(expectedPayouts);
    }

    @ParameterizedTest(name = "{0} pays {1} times after a crash between the effect and the bookkeeping")
    @CsvSource({"naive, 2", "check-then-record, 2", "idempotent, 1"})
    void crashAfterTheEffectThenRedelivery(String name, long expectedPayouts) {
        var db = Sinks.onH2();
        var sink = db.sink(name, 1.0); // every event crashes once, right after the payout
        var event = EVENTS.getFirst();

        assertThatThrownBy(() -> sink.pay(event)).isInstanceOf(SimulatedCrashException.class);
        sink.pay(event); // Kafka redelivers, because the offset was never committed

        assertThat(db.payoutsFor(event.eventId())).isEqualTo(expectedPayouts);
    }

    @Test
    void idempotentSinkRollsBackTheRecordWithThePayment() {
        var db = Sinks.onH2();
        var sink = db.sink("idempotent", 1.0);

        assertThatThrownBy(() -> sink.pay(EVENTS.getFirst())).isInstanceOf(SimulatedCrashException.class);

        assertThat(db.payouts()).isZero();
        assertThat(db.processed())
                .as("the record must not survive without its payment")
                .isZero();
    }

    @Test
    void checkThenRecordLeavesAPaymentWithNoRecord() {
        var db = Sinks.onH2();
        var sink = db.sink("check-then-record", 1.0);

        assertThatThrownBy(() -> sink.pay(EVENTS.getFirst())).isInstanceOf(SimulatedCrashException.class);

        assertThat(db.payouts()).isEqualTo(1);
        assertThat(db.processed()).as("the gap the redelivery falls through").isZero();
    }

    @Test
    void duplicatesAreCountedAsSkipped() {
        var db = Sinks.onH2();
        var sink = db.sink("idempotent", 0);

        EVENTS.forEach(sink::pay);
        EVENTS.forEach(sink::pay);

        assertThat(db.stats.snapshot().duplicatesSkipped()).isEqualTo(EVENTS.size());
    }
}
