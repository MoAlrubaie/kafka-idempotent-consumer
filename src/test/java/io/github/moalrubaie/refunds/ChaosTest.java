package io.github.moalrubaie.refunds;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class ChaosTest {

    private static Chaos chaos(double rate, long seed) {
        return new Chaos(
                new AppProperties("t", "t.DLT", 1, "naive", 3, Duration.ZERO, new AppProperties.Chaos(rate, seed)),
                new Stats());
    }

    @Test
    void offByDefault() {
        var chaos = chaos(0, 7);
        assertThatCode(() -> chaos.afterEffect("refund-1")).doesNotThrowAnyException();
    }

    @Test
    void picksTheSameEventsForTheSameSeed() {
        var a = chaos(0.2, 7);
        var b = chaos(0.2, 7);
        var c = chaos(0.2, 8);
        var ids = IntStream.range(0, 500).mapToObj(i -> "refund-" + i).toList();

        var chosenA = ids.stream().filter(a::isChosen).toList();

        assertThat(ids.stream().filter(b::isChosen).toList()).isEqualTo(chosenA);
        assertThat(ids.stream().filter(c::isChosen).toList()).isNotEqualTo(chosenA);
        assertThat(chosenA.size()).isBetween(70, 130); // about 20% of 500
    }

    @Test
    void crashesEachChosenEventOnlyOnce() {
        var stats = new Stats();
        var chaos = new Chaos(
                new AppProperties("t", "t.DLT", 1, "naive", 3, Duration.ZERO, new AppProperties.Chaos(1.0, 7)), stats);

        assertThatThrownBy(() -> chaos.afterEffect("refund-1")).isInstanceOf(Chaos.SimulatedCrashException.class);
        assertThatCode(() -> chaos.afterEffect("refund-1")).doesNotThrowAnyException();
        assertThat(stats.snapshot().crashes()).isEqualTo(1);
    }
}
