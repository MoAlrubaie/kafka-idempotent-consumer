package io.github.moalrubaie.refunds;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Settings under {@code app.*} in application.yml.
 *
 * @param topic the topic of approved refunds, keyed by order ID
 * @param deadLetterTopic where events go when they can't be processed
 * @param partitions partitions for both topics when the app creates them
 * @param sink which sink pays the refunds: naive, check-then-record or idempotent
 * @param attempts how many times to try an event before dead-lettering it
 * @param retryBackoff the pause between attempts
 * @param chaos fault injection, off by default
 */
@ConfigurationProperties("app")
public record AppProperties(
        String topic,
        String deadLetterTopic,
        int partitions,
        String sink,
        int attempts,
        Duration retryBackoff,
        Chaos chaos) {

    /**
     * @param crashAfterEffectRate the share of events (0 to 1) whose first delivery fails right after
     *     the payout is made, before the bookkeeping and the offset commit
     * @param seed picks which events those are, so every run and every sink sees the same ones
     */
    public record Chaos(double crashAfterEffectRate, long seed) {}
}
