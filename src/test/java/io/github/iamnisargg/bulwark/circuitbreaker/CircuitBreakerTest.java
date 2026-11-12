package io.github.iamnisargg.bulwark.circuitbreaker;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.iamnisargg.bulwark.ManualTimeSource;
import io.github.iamnisargg.bulwark.circuitbreaker.CircuitBreaker.State;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class CircuitBreakerTest {

    private final ManualTimeSource time = new ManualTimeSource();
    private final List<CircuitBreaker.StateChange> changes = new ArrayList<>();

    private CircuitBreaker.Builder breaker() {
        return CircuitBreaker.builder()
                .name("test")
                .slidingWindowSize(4)
                .minimumCalls(4)
                .failureRateThreshold(0.5)
                .openDuration(Duration.ofSeconds(10))
                .halfOpenCalls(2)
                .timeSource(time)
                .onStateChange(changes::add);
    }

    private static void succeed(CircuitBreaker breaker) throws Exception {
        breaker.execute(() -> "ok");
    }

    private static void fail(CircuitBreaker breaker) {
        assertThatThrownBy(() -> breaker.execute(() -> {
            throw new IOException("down");
        })).isInstanceOf(IOException.class);
    }

    private static void tripOpen(CircuitBreaker breaker) {
        for (int i = 0; i < 4; i++) {
            fail(breaker);
        }
        assertThat(breaker.state()).isEqualTo(State.OPEN);
    }

    @Test
    void staysClosedBelowMinimumCallsEvenIfEverythingFails() {
        CircuitBreaker breaker = breaker().build();

        for (int i = 0; i < 3; i++) {
            fail(breaker);
        }

        assertThat(breaker.state()).isEqualTo(State.CLOSED);
    }

    @Test
    void staysClosedWhenFailureRateIsBelowThreshold() throws Exception {
        CircuitBreaker breaker = breaker().failureRateThreshold(0.75).build();

        fail(breaker);
        fail(breaker);
        succeed(breaker);
        succeed(breaker);

        assertThat(breaker.state()).isEqualTo(State.CLOSED);
        assertThat(breaker.metrics().failureRate()).isEqualTo(0.5);
    }

    @Test
    void opensWhenFailureRateReachesThreshold() throws Exception {
        CircuitBreaker breaker = breaker().build();

        succeed(breaker);
        succeed(breaker);
        fail(breaker);
        fail(breaker);

        assertThat(breaker.state()).isEqualTo(State.OPEN);
    }

    @Test
    void windowSlidesSoOldFailuresAgeOut() throws Exception {
        CircuitBreaker breaker = breaker().failureRateThreshold(0.75).build();

        fail(breaker);
        fail(breaker);
        succeed(breaker);
        succeed(breaker);
        fail(breaker);
        fail(breaker);
        assertThat(breaker.state()).as("[S,S,F,F] is 50%").isEqualTo(State.CLOSED);
        fail(breaker);

        assertThat(breaker.state()).as("[S,F,F,F] is 75%; a cumulative counter would see only 71%")
                .isEqualTo(State.OPEN);
    }

    @Test
    void openBreakerRejectsWithoutInvokingTheActionAndReportsRetryAfter() {
        CircuitBreaker breaker = breaker().build();
        tripOpen(breaker);
        time.advance(Duration.ofSeconds(4));
        AtomicInteger invoked = new AtomicInteger();

        assertThatThrownBy(() -> breaker.execute(() -> invoked.incrementAndGet()))
                .isInstanceOfSatisfying(CircuitBreakerOpenException.class, e -> {
                    assertThat(e.breakerName()).isEqualTo("test");
                    assertThat(e.retryAfter()).isEqualTo(Duration.ofSeconds(6));
                });
        assertThat(invoked).hasValue(0);
        assertThat(breaker.metrics().notPermittedCalls()).isEqualTo(1);
    }

    @Test
    void movesToHalfOpenLazilyAfterTheWaitDuration() throws Exception {
        CircuitBreaker breaker = breaker().build();
        tripOpen(breaker);

        time.advance(Duration.ofSeconds(10));
        assertThat(breaker.state()).as("transition is lazy").isEqualTo(State.OPEN);
        succeed(breaker);

        assertThat(breaker.state()).isEqualTo(State.HALF_OPEN);
    }

    @Test
    void closesAfterAllTrialCallsSucceed() throws Exception {
        CircuitBreaker breaker = breaker().build();
        tripOpen(breaker);
        time.advance(Duration.ofSeconds(10));

        succeed(breaker);
        succeed(breaker);

        assertThat(breaker.state()).isEqualTo(State.CLOSED);
        assertThat(breaker.metrics().bufferedCalls()).as("statistics start fresh").isZero();
        assertThat(changes).extracting(CircuitBreaker.StateChange::to)
                .containsExactly(State.OPEN, State.HALF_OPEN, State.CLOSED);
    }

    @Test
    void reopensImmediatelyOnTrialFailuresWithAFreshWait() throws Exception {
        CircuitBreaker breaker = breaker().halfOpenCalls(2).build();
        tripOpen(breaker);
        time.advance(Duration.ofSeconds(10));

        fail(breaker);

        assertThat(breaker.state()).as("1 of 2 trial calls failed = 50%, no need to wait for the second")
                .isEqualTo(State.OPEN);
        time.advance(Duration.ofSeconds(9));
        assertThatThrownBy(() -> succeed(breaker)).isInstanceOf(CircuitBreakerOpenException.class);
        time.advance(Duration.ofSeconds(1));
        succeed(breaker);
        assertThat(breaker.state()).isEqualTo(State.HALF_OPEN);
    }

    @Test
    void halfOpenAdmitsOnlyTheConfiguredNumberOfTrialCalls() throws Exception {
        CircuitBreaker breaker = breaker().halfOpenCalls(2).build();
        tripOpen(breaker);
        time.advance(Duration.ofSeconds(10));

        Object outcome = breaker.execute(() -> breaker.execute(() -> {
            assertThatThrownBy(() -> breaker.execute(() -> "third"))
                    .isInstanceOf(CircuitBreakerOpenException.class);
            return "inner";
        }));

        assertThat(outcome).isEqualTo("inner");
        assertThat(breaker.state()).isEqualTo(State.CLOSED);
    }

    @Test
    void lateResultFromAPreviousStateIsDiscarded() throws Exception {
        CircuitBreaker breaker = breaker().build();

        String result = breaker.execute(() -> {
            tripOpen(breaker);
            return "late success";
        });

        assertThat(result).isEqualTo("late success");
        assertThat(breaker.state()).as("a success admitted while CLOSED must not close an OPEN breaker")
                .isEqualTo(State.OPEN);
    }

    @Test
    void exceptionsRejectedByThePredicateCountAsSuccess() {
        CircuitBreaker breaker = breaker()
                .recordFailureIf(t -> t instanceof IOException)
                .build();

        for (int i = 0; i < 8; i++) {
            assertThatThrownBy(() -> breaker.execute(() -> {
                throw new IllegalArgumentException("client error");
            })).isInstanceOf(IllegalArgumentException.class);
        }

        assertThat(breaker.state()).isEqualTo(State.CLOSED);
        assertThat(breaker.metrics().failedCalls()).isZero();
    }

    @Test
    void forceOpenAndResetAreIdempotentManualOverrides() {
        CircuitBreaker breaker = breaker().build();

        breaker.forceOpen();
        breaker.forceOpen();
        assertThat(breaker.state()).isEqualTo(State.OPEN);
        breaker.reset();
        breaker.reset();

        assertThat(breaker.state()).isEqualTo(State.CLOSED);
        assertThat(changes).hasSize(2);
    }

    @Test
    void aFailingListenerCannotBreakTheGuardedCall() throws Exception {
        CircuitBreaker breaker = breaker().onStateChange(change -> {
            throw new IllegalStateException("listener bug");
        }).build();

        tripOpen(breaker);

        assertThat(breaker.state()).isEqualTo(State.OPEN);
    }

    @Test
    void rejectsInvalidConfiguration() {
        assertThatThrownBy(() -> CircuitBreaker.builder().failureRateThreshold(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> CircuitBreaker.builder().failureRateThreshold(1.1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> CircuitBreaker.builder().slidingWindowSize(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> CircuitBreaker.builder().halfOpenCalls(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> CircuitBreaker.builder().openDuration(Duration.ZERO)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> CircuitBreaker.builder().slidingWindowSize(5).minimumCalls(6).build())
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void underContentionOnlyAFewCallsLeakThroughOnceTheBreakerTrips() throws Exception {
        int threads = 16;
        int callsPerThread = 2_000;
        int windowSize = 10;
        CircuitBreaker breaker = CircuitBreaker.builder()
                .slidingWindowSize(windowSize)
                .minimumCalls(windowSize)
                .failureRateThreshold(0.5)
                .openDuration(Duration.ofHours(1))
                .timeSource(time)
                .build();
        AtomicInteger invoked = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    for (int i = 0; i < callsPerThread; i++) {
                        try {
                            breaker.execute(() -> {
                                invoked.incrementAndGet();
                                throw new IOException("down");
                            });
                        } catch (CircuitBreakerOpenException e) {
                            rejected.incrementAndGet();
                        } catch (IOException expected) {
                            // the guarded action failing is the point of this test
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

        assertThat(breaker.state()).isEqualTo(State.OPEN);
        assertThat(invoked.get() + rejected.get()).isEqualTo(threads * callsPerThread);
        assertThat(invoked.get()).as("window of %d plus at most one in-flight call per thread", windowSize)
                .isLessThanOrEqualTo(windowSize + threads);
    }
}
