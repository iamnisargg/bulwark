package io.github.iamnisargg.bulwark.retry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.Random;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class BackoffTest {

    @Test
    void noneAndFixed() {
        assertThat(Backoff.none().delay(5)).isEqualTo(Duration.ZERO);
        assertThat(Backoff.fixed(Duration.ofMillis(250)).delay(1)).isEqualTo(Duration.ofMillis(250));
        assertThat(Backoff.fixed(Duration.ofMillis(250)).delay(9)).isEqualTo(Duration.ofMillis(250));
    }

    @Test
    void exponentialGrowsThenCapsAtMax() {
        Backoff backoff = Backoff.exponential(Duration.ofMillis(100), 2.0, Duration.ofMillis(500));

        assertThat(backoff.delay(1)).isEqualTo(Duration.ofMillis(100));
        assertThat(backoff.delay(2)).isEqualTo(Duration.ofMillis(200));
        assertThat(backoff.delay(3)).isEqualTo(Duration.ofMillis(400));
        assertThat(backoff.delay(4)).isEqualTo(Duration.ofMillis(500));
        assertThat(backoff.delay(5)).isEqualTo(Duration.ofMillis(500));
    }

    @Test
    void exponentialCannotOverflowForHugeRetryNumbers() {
        Backoff backoff = Backoff.exponential(Duration.ofSeconds(1), 10.0, Duration.ofMinutes(1));

        assertThat(backoff.delay(Integer.MAX_VALUE)).isEqualTo(Duration.ofMinutes(1));
    }

    @Test
    void fullJitterStaysWithinZeroAndBaseDelay() {
        Backoff jittered = Backoff.fixed(Duration.ofMillis(100)).withFullJitter(new Random(7));

        for (int i = 0; i < 2_000; i++) {
            assertThat(jittered.delay(1)).isBetween(Duration.ZERO, Duration.ofMillis(100));
        }
    }

    @Test
    void fullJitterActuallyVaries() {
        Backoff jittered = Backoff.fixed(Duration.ofMillis(100)).withFullJitter(new Random(7));

        assertThat(IntStream.range(0, 50).mapToObj(jittered::delay).distinct().count())
                .isGreaterThan(10);
    }

    @Test
    void equalJitterKeepsAtLeastHalfTheDelay() {
        Backoff jittered = Backoff.fixed(Duration.ofMillis(100)).withEqualJitter(new Random(7));

        for (int i = 0; i < 2_000; i++) {
            assertThat(jittered.delay(1)).isBetween(Duration.ofMillis(50), Duration.ofMillis(100));
        }
    }

    @Test
    void jitterOfZeroDelayIsZero() {
        assertThat(Backoff.none().withFullJitter().delay(1)).isEqualTo(Duration.ZERO);
        assertThat(Backoff.none().withEqualJitter().delay(1)).isEqualTo(Duration.ZERO);
    }

    @Test
    void rejectsInvalidConfiguration() {
        assertThatThrownBy(() -> Backoff.fixed(Duration.ofMillis(-1))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Backoff.exponential(Duration.ofMillis(1), 0.5, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Backoff.exponential(Duration.ofSeconds(2), 2, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
