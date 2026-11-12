package io.github.iamnisargg.bulwark.ratelimit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.iamnisargg.bulwark.ManualTimeSource;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class TokenBucketRateLimiterTest {

    private final ManualTimeSource time = new ManualTimeSource();

    private TokenBucketRateLimiter limiter(long capacity, long permits, Duration period) {
        return TokenBucketRateLimiter.builder()
                .capacity(capacity)
                .refillRate(permits, period)
                .timeSource(time)
                .build();
    }

    @Test
    void startsFullAndAllowsBurstUpToCapacity() {
        TokenBucketRateLimiter limiter = limiter(5, 1, Duration.ofSeconds(1));

        for (int i = 0; i < 5; i++) {
            assertThat(limiter.tryAcquire()).as("permit %d", i).isTrue();
        }
        assertThat(limiter.tryAcquire()).isFalse();
    }

    @Test
    void rejectionReportsExactRetryAfter() {
        TokenBucketRateLimiter limiter = limiter(2, 2, Duration.ofSeconds(1));
        limiter.tryAcquire(2);

        RateLimiter.Decision decision = limiter.evaluate(1);

        assertThat(decision.allowed()).isFalse();
        assertThat(decision.retryAfter()).isEqualTo(Duration.ofMillis(500));

        time.advance(Duration.ofMillis(499));
        assertThat(limiter.evaluate(1).retryAfter()).isEqualTo(Duration.ofMillis(1));
        time.advance(Duration.ofMillis(1));
        assertThat(limiter.tryAcquire()).isTrue();
    }

    @Test
    void refillsContinuouslyNotInSteps() {
        TokenBucketRateLimiter limiter = limiter(10, 10, Duration.ofSeconds(1));
        limiter.tryAcquire(10);

        time.advance(Duration.ofMillis(300));

        assertThat(limiter.availablePermits()).isEqualTo(3);
        assertThat(limiter.tryAcquire(3)).isTrue();
        assertThat(limiter.tryAcquire()).isFalse();
    }

    @Test
    void neverAccumulatesMoreThanCapacity() {
        TokenBucketRateLimiter limiter = limiter(3, 1, Duration.ofSeconds(1));

        time.advance(Duration.ofHours(1));

        assertThat(limiter.availablePermits()).isEqualTo(3);
        assertThat(limiter.tryAcquire(3)).isTrue();
        assertThat(limiter.tryAcquire()).isFalse();
    }

    @Test
    void capacityDefaultsToRefillPermits() {
        TokenBucketRateLimiter limiter = TokenBucketRateLimiter.builder()
                .refillRate(4, Duration.ofSeconds(1))
                .timeSource(time)
                .build();

        assertThat(limiter.capacity()).isEqualTo(4);
    }

    @Test
    void rejectedRequestDoesNotConsumePermits() {
        TokenBucketRateLimiter limiter = limiter(4, 1, Duration.ofSeconds(1));
        limiter.tryAcquire(3);

        assertThat(limiter.tryAcquire(2)).isFalse();
        assertThat(limiter.tryAcquire(1)).isTrue();
    }

    @Test
    void rejectsInvalidArguments() {
        TokenBucketRateLimiter limiter = limiter(2, 1, Duration.ofSeconds(1));

        assertThatThrownBy(() -> limiter.tryAcquire(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> limiter.tryAcquire(3)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TokenBucketRateLimiter.builder().build()).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> TokenBucketRateLimiter.builder().refillRate(0, Duration.ofSeconds(1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TokenBucketRateLimiter.builder().refillRate(1, Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void asPolicyThrowsWithRetryAfterWhenExhausted() throws Exception {
        TokenBucketRateLimiter limiter = limiter(1, 1, Duration.ofSeconds(1));
        var policy = limiter.asPolicy();

        assertThat(policy.execute(() -> "ok")).isEqualTo("ok");
        assertThatThrownBy(() -> policy.execute(() -> "nope"))
                .isInstanceOfSatisfying(RateLimitExceededException.class,
                        e -> assertThat(e.retryAfter()).isEqualTo(Duration.ofSeconds(1)));
    }

    @Test
    void neverGrantsMoreThanCapacityUnderContention() throws Exception {
        int threads = 32;
        int attemptsPerThread = 10_000;
        long capacity = 1_000;
        TokenBucketRateLimiter limiter = limiter(capacity, 1, Duration.ofHours(1));
        AtomicInteger granted = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    for (int i = 0; i < attemptsPerThread; i++) {
                        if (limiter.tryAcquire()) {
                            granted.incrementAndGet();
                        }
                    }
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> future : futures) {
                future.get();
            }
        } finally {
            pool.shutdownNow();
        }

        assertThat(granted.get()).isEqualTo((int) capacity);
    }
}
