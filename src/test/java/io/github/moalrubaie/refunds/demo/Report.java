package io.github.moalrubaie.refunds.demo;

import io.github.moalrubaie.refunds.demo.DemoRunner.Round;
import io.github.moalrubaie.refunds.demo.DemoRunner.SinkResult;

/** Formats a {@link DemoRunner.Result} as the plain-text tables {@code make demo} prints. */
final class Report {

    private Report() {}

    static String render(DemoRunner.Result result, DemoRunner.Options options) {
        var p = result.published();
        var out = new StringBuilder();
        out.append("kafka-idempotent-consumer  broker=%s  db=%s  seed=%d%n%n"
                .formatted(result.broker(), result.database(), options.seed()));
        out.append("Stream: %,d refunds worth %s on %s, keyed by order ID (%d partitions)%n"
                .formatted(p.refunds(), money(p.expectedCents()), DemoRunner.TOPIC, DemoRunner.PARTITIONS));
        out.append("  + %-12s the upstream retried and published the same event again%n"
                .formatted("%,d resends".formatted(p.resends())));
        out.append("  + %-12s bad JSON or a negative amount, can never succeed%n"
                .formatted("%,d poison".formatted(p.poison())));
        out.append("  chaos: %.0f%% of events crash once right after the payout, before the bookkeeping%n%n"
                .formatted(options.crashRate() * 100));

        out.append("Round 1: deliver the stream once%n".formatted());
        out.append(header());
        for (SinkResult s : result.sinks()) {
            out.append(row(s.sink(), s.firstPass()));
        }
        out.append("%nRound 2: rewind the consumer group to the start and replay the whole topic%n".formatted());
        out.append(header());
        for (SinkResult s : result.sinks()) {
            out.append(row(s.sink(), s.replay()));
        }
        out.append("%n".formatted());
        for (SinkResult s : result.sinks()) {
            out.append("  %-18s %s%n".formatted(s.sink(), verdict(s)));
        }
        return out.toString();
    }

    private static String header() {
        return "  %-18s %10s %8s %8s %6s %8s %13s %13s%n"
                .formatted("sink", "deliveries", "crashes", "skipped", "DLT", "payouts", "paid out", "overpaid");
    }

    private static String row(String sink, Round r) {
        return "  %-18s %,10d %,8d %,8d %,6d %,8d %13s %13s%n"
                .formatted(
                        sink,
                        r.deliveries(),
                        r.crashes(),
                        r.duplicatesSkipped(),
                        r.deadLettered(),
                        r.payouts(),
                        money(r.paidCents()),
                        r.overpaidCents() == 0 ? "-" : "+" + money(r.overpaidCents()));
    }

    private static String verdict(SinkResult s) {
        Round first = s.firstPass();
        Round replay = s.replay();
        if (first.exact() && replay.exact()) {
            return "every refund paid exactly once, through %,d deliveries"
                    .formatted(first.deliveries() + replay.deliveries());
        }
        var text = new StringBuilder();
        if (!first.exact()) {
            text.append("paid %,d refunds more than once in round 1".formatted(first.doublePaid()));
        } else {
            text.append("exact in round 1");
        }
        long extra = replay.payouts() - first.payouts();
        text.append(extra > 0 ? ", paid %,d more on the replay".formatted(extra) : ", replay changed nothing");
        if (replay.unpaid() > 0) {
            text.append(", %,d refunds never paid".formatted(replay.unpaid()));
        }
        return text.append(" (%s too much)".formatted(money(replay.overpaidCents())))
                .toString();
    }

    static String money(long cents) {
        return "%s$%,d.%02d".formatted(cents < 0 ? "-" : "", Math.abs(cents) / 100, Math.abs(cents) % 100);
    }
}
