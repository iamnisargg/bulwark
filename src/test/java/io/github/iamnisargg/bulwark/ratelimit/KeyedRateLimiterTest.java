package io.github.iamnisargg.bulwark.ratelimit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.iamnisargg.bulwark.ManualTimeSource;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class KeyedRateLimiterTest {

    private final ManualTimeSource time = new ManualTimeSource();

    private KeyedRateLimiter<String> perKey(long permits, Duration refillPeriod, Duration maxIdle) {
        return new KeyedRateLimiter<>(
                key -> TokenBucketRateLimiter.builder()
                        .refillRate(permits, refillPeriod)
                        .timeSource(time)
                        .build(),
                maxIdle,
                time);
    }

    private KeyedRateLimiter<String> perKey(long permitsPerSecond, Duration maxIdle) {
        return perKey(permitsPerSecond, Duration.ofSeconds(1), maxIdle);
    }

    @Test
    void keysAreLimitedIndependently() {
        KeyedRateLimiter<String> limiter = perKey(1, Duration.ofMinutes(1));

        assertThat(limiter.tryAcquire("alice")).isTrue();
        assertThat(limiter.tryAcquire("alice")).isFalse();
        assertThat(limiter.tryAcquire("bob")).isTrue();
        assertThat(limiter.trackedKeys()).isEqualTo(2);
    }

    @Test
    void idleKeysAreEvictedAndActiveKeysSurvive() {
        KeyedRateLimiter<String> limiter = perKey(1, Duration.ofSeconds(10));
        limiter.tryAcquire("idle");
        limiter.tryAcquire("active");

        time.advance(Duration.ofSeconds(6));
        limiter.tryAcquire("active");
        time.advance(Duration.ofSeconds(6));
        limiter.tryAcquire("trigger-sweep");

        assertThat(limiter.trackedKeys()).isEqualTo(2);
    }

    @Test
    void evictedKeyStartsWithAFreshBucket() {
        KeyedRateLimiter<String> limiter = perKey(1, Duration.ofHours(1), Duration.ofSeconds(5));
        limiter.tryAcquire("k");
        assertThat(limiter.tryAcquire("k")).isFalse();

        time.advance(Duration.ofSeconds(4));
        assertThat(limiter.tryAcquire("k")).as("still tracked, still empty").isFalse();

        time.advance(Duration.ofSeconds(5));
        limiter.tryAcquire("other");

        assertThat(limiter.tryAcquire("k")).as("evicted, so a fresh bucket").isTrue();
    }

    @Test
    void rejectsInvalidArguments() {
        assertThatThrownBy(() -> perKey(1, Duration.ZERO)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> perKey(1, Duration.ofSeconds(1)).tryAcquire(null))
                .isInstanceOf(NullPointerException.class);
    }
}
