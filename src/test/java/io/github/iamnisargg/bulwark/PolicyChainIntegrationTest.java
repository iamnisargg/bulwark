package io.github.iamnisargg.bulwark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.iamnisargg.bulwark.bulkhead.Bulkhead;
import io.github.iamnisargg.bulwark.bulkhead.BulkheadFullException;
import io.github.iamnisargg.bulwark.circuitbreaker.CircuitBreaker;
import io.github.iamnisargg.bulwark.circuitbreaker.CircuitBreaker.State;
import io.github.iamnisargg.bulwark.circuitbreaker.CircuitBreakerOpenException;
import io.github.iamnisargg.bulwark.ratelimit.RateLimitExceededException;
import io.github.iamnisargg.bulwark.ratelimit.RateLimiter;
import io.github.iamnisargg.bulwark.ratelimit.TokenBucketRateLimiter;
import io.github.iamnisargg.bulwark.retry.Backoff;
import io.github.iamnisargg.bulwark.retry.Retry;
import io.github.iamnisargg.bulwark.retry.RetryExhaustedException;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Exercises the recommended composition: retry, then circuit breaker, then rate limiter, then bulkhead,
 * then the action. Time is virtual: the retry "sleeper" advances the same clock the breaker and the
 * rate limiter read, so back-off delays really do move the breaker towards its half-open window.
 */
class PolicyChainIntegrationTest {

    private static final Duration BACKOFF = Duration.ofMillis(100);
    private static final Duration OPEN_DURATION = Duration.ofSeconds(10);

    private final ManualTimeSource clock = new ManualTimeSource();
    private final List<Duration> delays = new CopyOnWriteArrayList<>();
    private final List<String> transitions = new CopyOnWriteArrayList<>();
    private final ExecutorService pool = Executors.newCachedThreadPool();

    private CircuitBreaker breaker;

    @AfterEach
    void shutDown() {
        pool.shutdownNow();
    }

    private Policy chain(RateLimiter limiter, Bulkhead bulkhead) {
        Retry retry = Retry.builder()
                .maxAttempts(3)
                .backoff(Backoff.fixed(BACKOFF))
                // Fail fast on local protection: retrying an open breaker or a full bulkhead only adds load.
                .abortOn(CircuitBreakerOpenException.class, RateLimitExceededException.class,
                        BulkheadFullException.class)
                .sleeper(duration -> {
                    delays.add(duration);
                    clock.advance(duration);
                })
                .build();
        breaker = CircuitBreaker.builder()
                .name("downstream")
                .slidingWindowSize(4)
                .minimumCalls(4)
                .failureRateThreshold(0.5)
                .openDuration(OPEN_DURATION)
                .halfOpenCalls(1)
                // Our own load shedding says nothing about the health of the dependency.
                .recordFailureIf(t -> !(t instanceof RateLimitExceededException
                        || t instanceof BulkheadFullException))
                .onStateChange(change -> transitions.add(change.from() + "->" + change.to()))
                .timeSource(clock)
                .build();
        return Policy.chain(retry, breaker, limiter.asPolicy(), bulkhead);
    }

    private Policy chain() {
        return chain(plentyOfTokens(), Bulkhead.builder().maxConcurrentCalls(8).build());
    }

    private RateLimiter plentyOfTokens() {
        return limiter(1_000);
    }

    private RateLimiter limiter(long capacity) {
        return TokenBucketRateLimiter.builder()
                .capacity(capacity)
                .refillRate(1, Duration.ofHours(1))
                .timeSource(clock)
                .build();
    }

    @Test
    void retryRecoversFromTransientFailuresWithoutTrippingTheBreaker() throws Exception {
        Policy policy = chain();
        AtomicInteger calls = new AtomicInteger();

        String result = policy.execute(() -> {
            if (calls.incrementAndGet() < 3) {
                throw new IOException("transient");
            }
            return "ok";
        });

        assertThat(result).isEqualTo("ok");
        assertThat(calls).hasValue(3);
        assertThat(delays).containsExactly(BACKOFF, BACKOFF);
        assertThat(breaker.state()).isEqualTo(State.CLOSED);
        assertThat(breaker.metrics().failedCalls()).isEqualTo(2);
    }

    @Test
    void persistentFailureTripsTheBreakerAndLaterCallsNeverReachTheAction() throws Exception {
        Policy policy = chain();
        AtomicInteger calls = new AtomicInteger();
        CheckedSupplier<String> failing = () -> {
            calls.incrementAndGet();
            throw new IOException("down");
        };

        assertThatThrownBy(() -> policy.execute(failing))
                .isInstanceOfSatisfying(RetryExhaustedException.class, e -> assertThat(e.attempts()).isEqualTo(3));
        assertThat(breaker.state()).isEqualTo(State.CLOSED);

        // The fourth recorded failure fills the window and opens the breaker mid-retry; the next
        // attempt is rejected and, being non-retryable, surfaces as-is instead of burning attempts.
        assertThatThrownBy(() -> policy.execute(failing)).isInstanceOf(CircuitBreakerOpenException.class);
        assertThat(breaker.state()).isEqualTo(State.OPEN);
        assertThat(calls).hasValue(4);

        int delaysBefore = delays.size();
        assertThatThrownBy(() -> policy.execute(failing)).isInstanceOf(CircuitBreakerOpenException.class);
        assertThat(calls).hasValue(4);
        assertThat(delays).hasSize(delaysBefore);
    }

    @Test
    void breakerHealsOnceTheDependencyRecoversAfterTheOpenWindow() throws Exception {
        Policy policy = chain();
        breaker.forceOpen();
        assertThatThrownBy(() -> policy.execute(() -> "x")).isInstanceOf(CircuitBreakerOpenException.class);

        clock.advance(OPEN_DURATION);
        String result = policy.execute(() -> "recovered");

        assertThat(result).isEqualTo("recovered");
        assertThat(breaker.state()).isEqualTo(State.CLOSED);
        assertThat(transitions).containsExactly("CLOSED->OPEN", "OPEN->HALF_OPEN", "HALF_OPEN->CLOSED");
    }

    @Test
    void rateLimitRejectionsAreNotRetriedAndDoNotCountAgainstTheBreaker() throws Exception {
        Policy policy = chain(limiter(2), Bulkhead.builder().maxConcurrentCalls(8).build());
        AtomicInteger calls = new AtomicInteger();
        CheckedSupplier<String> action = () -> {
            calls.incrementAndGet();
            return "ok";
        };

        assertThat(policy.execute(action)).isEqualTo("ok");
        assertThat(policy.execute(action)).isEqualTo("ok");
        assertThatThrownBy(() -> policy.execute(action))
                .isInstanceOfSatisfying(RateLimitExceededException.class,
                        e -> assertThat(e.retryAfter()).isPositive());

        assertThat(calls).hasValue(2);
        assertThat(delays).isEmpty();
        assertThat(breaker.metrics().failedCalls()).isZero();
        assertThat(breaker.state()).isEqualTo(State.CLOSED);
    }

    @Test
    void bulkheadRejectionsAreNotRetriedAndDoNotCountAgainstTheBreaker() throws Exception {
        Bulkhead bulkhead = Bulkhead.builder().name("pool").maxConcurrentCalls(1).build();
        Policy policy = chain(plentyOfTokens(), bulkhead);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Future<String> holder = pool.submit(() -> policy.execute(() -> {
            entered.countDown();
            release.await();
            return "slow";
        }));
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();

        assertThatThrownBy(() -> policy.execute(() -> "x")).isInstanceOf(BulkheadFullException.class);

        assertThat(delays).isEmpty();
        assertThat(breaker.metrics().failedCalls()).isZero();
        release.countDown();
        assertThat(holder.get(5, TimeUnit.SECONDS)).isEqualTo("slow");
        assertThat(bulkhead.availablePermits()).isEqualTo(1);
    }

    @Test
    void concurrentTrafficAgainstAFlakyDependencyNeverBreaksInvariants() throws Exception {
        int slots = 4;
        Bulkhead bulkhead = Bulkhead.builder().maxConcurrentCalls(slots).maxWait(Duration.ofSeconds(10)).build();
        Policy policy = chain(limiter(10_000), bulkhead);
        AtomicInteger sequence = new AtomicInteger();
        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger peak = new AtomicInteger();

        Callable<Object> caller = () -> {
            try {
                return policy.execute(() -> {
                    peak.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
                    try {
                        if (sequence.incrementAndGet() % 4 == 0) {
                            throw new IOException("flaky");
                        }
                        return "ok";
                    } finally {
                        inFlight.decrementAndGet();
                    }
                });
            } catch (Exception e) {
                return e;
            }
        };
        List<Future<Object>> futures = new ArrayList<>();
        for (int i = 0; i < 400; i++) {
            futures.add(pool.submit(caller));
        }

        for (Future<Object> future : futures) {
            Object outcome = future.get(30, TimeUnit.SECONDS);
            assertThat(outcome).satisfiesAnyOf(
                    o -> assertThat(o).isEqualTo("ok"),
                    o -> assertThat(o).isInstanceOf(RetryExhaustedException.class),
                    o -> assertThat(o).isInstanceOf(CircuitBreakerOpenException.class));
        }

        assertThat(peak.get()).isBetween(1, slots);
        assertThat(bulkhead.availablePermits()).isEqualTo(slots);
        assertThat(bulkhead.rejectedCalls()).isZero();
    }
}
