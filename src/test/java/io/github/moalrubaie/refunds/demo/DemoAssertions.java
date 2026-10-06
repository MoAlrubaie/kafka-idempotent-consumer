package io.github.moalrubaie.refunds.demo;

import static org.assertj.core.api.Assertions.assertThat;

/** What every end-to-end run must show, whichever broker and database it used. */
final class DemoAssertions {

    private DemoAssertions() {}

    static void assertExpectedOutcome(DemoRunner.Result result) {
        var published = result.published();
        var naive = result.sink("naive");
        var checkThenRecord = result.sink("check-then-record");
        var idempotent = result.sink("idempotent");

        // The idempotent sink pays every refund exactly once, through resends, crashes and a full replay.
        assertThat(idempotent.firstPass().exact()).as("idempotent, round 1").isTrue();
        assertThat(idempotent.replay().exact()).as("idempotent, replay").isTrue();
        assertThat(idempotent.firstPass().duplicatesSkipped()).isEqualTo(published.resends());

        // The naive sink pays every delivery that got as far as the payout.
        long crashes = naive.firstPass().crashes();
        assertThat(crashes).isPositive();
        assertThat(naive.firstPass().payouts()).isEqualTo(published.refunds() + published.resends() + crashes);
        assertThat(naive.replay().payouts())
                .isEqualTo(naive.firstPass().payouts() + published.refunds() + published.resends());

        // Check-then-record stops resends and replays, but not a crash between paying and recording.
        assertThat(checkThenRecord.firstPass().payouts()).isEqualTo(published.refunds() + crashes);
        assertThat(checkThenRecord.firstPass().doublePaid()).isEqualTo(crashes);
        assertThat(checkThenRecord.replay().payouts())
                .isEqualTo(checkThenRecord.firstPass().payouts());

        for (var sink : result.sinks()) {
            // The same events crash for every sink, and poison is parked once per pass, never retried forever.
            assertThat(sink.firstPass().crashes()).as(sink.sink() + " crashes").isEqualTo(crashes);
            assertThat(sink.firstPass().deadLettered()).as(sink.sink()).isEqualTo(published.poison());
            assertThat(sink.replay().deadLettered()).as(sink.sink()).isEqualTo(published.poison());
            // Every record was delivered, plus one retry for each crash.
            assertThat(sink.firstPass().deliveries()).isEqualTo(published.records() + crashes);
            assertThat(sink.replay().deliveries()).isEqualTo(published.records());
        }
    }
}
