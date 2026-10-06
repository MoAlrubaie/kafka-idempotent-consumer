package io.github.moalrubaie.refunds;

import java.util.concurrent.atomic.AtomicLong;
import org.springframework.stereotype.Component;

/** In-process counters, read by the demo and the tests. */
@Component
public class Stats {

    private final AtomicLong deliveries = new AtomicLong();
    private final AtomicLong duplicatesSkipped = new AtomicLong();
    private final AtomicLong crashes = new AtomicLong();
    private final AtomicLong deadLettered = new AtomicLong();

    public void delivered() {
        deliveries.incrementAndGet();
    }

    public void duplicateSkipped() {
        duplicatesSkipped.incrementAndGet();
    }

    public void crashed() {
        crashes.incrementAndGet();
    }

    public void deadLettered() {
        deadLettered.incrementAndGet();
    }

    public Snapshot snapshot() {
        return new Snapshot(deliveries.get(), duplicatesSkipped.get(), crashes.get(), deadLettered.get());
    }

    /**
     * @param deliveries records handed to the listener, including retries and redeliveries
     * @param duplicatesSkipped deliveries the sink recognised as already paid
     * @param crashes simulated crashes after the effect
     * @param deadLettered records sent to the dead-letter topic
     */
    public record Snapshot(long deliveries, long duplicatesSkipped, long crashes, long deadLettered) {

        public Snapshot minus(Snapshot earlier) {
            return new Snapshot(
                    deliveries - earlier.deliveries,
                    duplicatesSkipped - earlier.duplicatesSkipped,
                    crashes - earlier.crashes,
                    deadLettered - earlier.deadLettered);
        }
    }
}
