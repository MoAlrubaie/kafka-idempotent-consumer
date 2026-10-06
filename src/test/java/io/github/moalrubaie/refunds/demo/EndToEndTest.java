package io.github.moalrubaie.refunds.demo;

import org.junit.jupiter.api.Test;

/** The whole service against an in-process Kafka broker and H2. No Docker needed. */
class EndToEndTest {

    @Test
    void onlyTheIdempotentSinkPaysEveryRefundExactlyOnce() throws Exception {
        var result = DemoRunner.run(DemoRunner.Options.defaults().withEvents(400), line -> {});

        DemoAssertions.assertExpectedOutcome(result);
    }
}
