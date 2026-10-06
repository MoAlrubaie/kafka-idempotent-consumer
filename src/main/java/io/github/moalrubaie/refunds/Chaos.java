package io.github.moalrubaie.refunds;

import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.stereotype.Component;

/**
 * The chaos switch. When on, it fails a seeded share of events at the worst moment: after the
 * payout has been made, before the consumer has recorded that it did it. Kafka then redelivers the
 * event, exactly as it would after a crash or a rebalance at that point.
 *
 * <p>Each chosen event fails once per process, so the retry gets through.
 */
@Component
public class Chaos {

    private final double rate;
    private final long seed;
    private final Stats stats;
    private final Set<String> alreadyCrashed = ConcurrentHashMap.newKeySet();

    public Chaos(AppProperties props, Stats stats) {
        this.rate = props.chaos().crashAfterEffectRate();
        this.seed = props.chaos().seed();
        this.stats = stats;
    }

    /** Call right after the side effect. Throws {@link SimulatedCrashException} for chosen events. */
    public void afterEffect(String eventId) {
        if (isChosen(eventId) && alreadyCrashed.add(eventId)) {
            stats.crashed();
            throw new SimulatedCrashException(eventId);
        }
    }

    /** Whether this event is one that crashes after its effect. The same for every run with a seed. */
    public boolean isChosen(String eventId) {
        return rate > 0 && new SplittableRandom(seed ^ eventId.hashCode()).nextDouble() < rate;
    }

    public static class SimulatedCrashException extends RuntimeException {
        public SimulatedCrashException(String eventId) {
            super("chaos: crashed after paying " + eventId + ", before recording it");
        }
    }
}
