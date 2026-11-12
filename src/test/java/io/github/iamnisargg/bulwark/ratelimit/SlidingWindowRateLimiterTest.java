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

class SlidingWindowRateLimiterTest {

    private final ManualTimeSource time = new ManualTimeSource();

    private SlidingWindowRateLimiter limiter(long limit, Duration window) {
        return new SlidingWindowRateLimiter(limit, window, time);
    }

    @Test
    void allowsUpToLimitWithinWindow() {
        SlidingWindowRateLimiter limiter = limiter(5, Duration.ofSeconds(1));

        for (int i = 0; i < 5; i++) {
            assertThat(limiter.tryAcquire()).isTrue();
        }
        assertThat(limiter.tryAcquire()).isFalse();
    }

    @Test
    void doesNotAllowDoubleBurstAroundWindowBoundary() {
        SlidingWindowRateLimiter limiter = limiter(10, Duration.ofSeconds(1));
        time.advance(Duration.ofMillis(900));
        for (int i = 0; i < 10; i++) {
            assertThat(limiter.tryAcquire()).isTrue();
        }

        time.advance(Duration.ofMillis(150));

        assertThat(limiter.tryAcquire()).as("a fixed-window counter would allow another full burst here").isFalse();
    }

    @Test
    void retryAfterPointsAtTheFirstInstantTheRequestFits() {
        SlidingWindowRateLimiter limiter = limiter(5, Duration.ofSeconds(1));
        limiter.tryAcquire(5);

        RateLimiter.Decision decision = limiter.evaluate(1);

        assertThat(decision.retryAfter()).isEqualTo(Duration.ofMillis(1200));

        time.advance(Duration.ofMillis(1199));
        assertThat(limiter.tryAcquire()).isFalse();
        time.advance(Duration.ofMillis(1));
        assertThat(limiter.tryAcquire()).isTrue();
    }

    @Test
    void retryAfterWithinSameWindowAccountsForPreviousWindowDecay() {
        SlidingWindowRateLimiter limiter = limiter(10, Duration.ofSeconds(1));
        limiter.tryAcquire(10);
        time.advance(Duration.ofSeconds(1));
        assertThat(limiter.tryAcquire(2)).isFalse();

        RateLimiter.Decision decision = limiter.evaluate(2);

        assertThat(decision.retryAfter()).isEqualTo(Duration.ofMillis(200));
        time.advance(decision.retryAfter());
        assertThat(limiter.tryAcquire(2)).isTrue();
    }

    @Test
    void fullyRecoversAfterTwoIdleWindows() {
        SlidingWindowRateLimiter limiter = limiter(3, Duration.ofSeconds(1));
        limiter.tryAcquire(3);

        time.advance(Duration.ofSeconds(2));

        assertThat(limiter.tryAcquire(3)).isTrue();
    }

    @Test
    void rejectsInvalidArguments() {
        SlidingWindowRateLimiter limiter = limiter(2, Duration.ofSeconds(1));

        assertThatThrownBy(() -> limiter.tryAcquire(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> limiter.tryAcquire(3)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> limiter(0, Duration.ofSeconds(1))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> limiter(1, Duration.ZERO)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void neverGrantsMoreThanLimitUnderContention() throws Exception {
        int threads = 32;
        long limit = 500;
        SlidingWindowRateLimiter limiter = limiter(limit, Duration.ofHours(1));
        AtomicInteger granted = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    for (int i = 0; i < 5_000; i++) {
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

        assertThat(granted.get()).isEqualTo((int) limit);
    }
}
