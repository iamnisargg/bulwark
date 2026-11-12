package io.github.iamnisargg.bulwark.retry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.iamnisargg.bulwark.CheckedSupplier;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class RetryTest {

    private final List<Duration> sleeps = new ArrayList<>();
    private final Sleeper recordingSleeper = sleeps::add;

    private Retry.Builder retry(int maxAttempts) {
        return Retry.builder()
                .maxAttempts(maxAttempts)
                .backoff(Backoff.exponential(Duration.ofMillis(100), 2.0, Duration.ofSeconds(1)))
                .sleeper(recordingSleeper);
    }

    private static CheckedSupplier<String> failingTimes(int failures, AtomicInteger calls, Exception failure) {
        return () -> {
            if (calls.incrementAndGet() <= failures) {
                throw failure;
            }
            return "ok";
        };
    }

    @Test
    void returnsImmediatelyOnSuccessWithoutSleeping() throws Exception {
        AtomicInteger calls = new AtomicInteger();

        String result = retry(3).build().execute(failingTimes(0, calls, new IOException()));

        assertThat(result).isEqualTo("ok");
        assertThat(calls).hasValue(1);
        assertThat(sleeps).isEmpty();
    }

    @Test
    void retriesWithBackoffUntilSuccess() throws Exception {
        AtomicInteger calls = new AtomicInteger();

        String result = retry(5).build().execute(failingTimes(3, calls, new IOException("flaky")));

        assertThat(result).isEqualTo("ok");
        assertThat(calls).hasValue(4);
        assertThat(sleeps).containsExactly(Duration.ofMillis(100), Duration.ofMillis(200), Duration.ofMillis(400));
    }

    @Test
    void exhaustionKeepsLastFailureAsCauseAndEarlierOnesSuppressed() {
        AtomicInteger calls = new AtomicInteger();
        CheckedSupplier<String> alwaysFails = () -> {
            throw new IOException("failure #" + calls.incrementAndGet());
        };

        assertThatThrownBy(() -> retry(3).build().execute(alwaysFails))
                .isInstanceOfSatisfying(RetryExhaustedException.class, e -> {
                    assertThat(e.attempts()).isEqualTo(3);
                    assertThat(e).hasCauseInstanceOf(IOException.class).hasRootCauseMessage("failure #3");
                    assertThat(e.getSuppressed()).extracting(Throwable::getMessage)
                            .containsExactly("failure #1", "failure #2");
                });
        assertThat(sleeps).hasSize(2);
    }

    @Test
    void singleAttemptNeverSleeps() {
        assertThatThrownBy(() -> retry(1).build().execute(() -> {
            throw new IOException();
        })).isInstanceOf(RetryExhaustedException.class);
        assertThat(sleeps).isEmpty();
    }

    @Test
    void nonRetryableFailureIsRethrownUnchangedAndImmediately() {
        AtomicInteger calls = new AtomicInteger();
        IllegalArgumentException failure = new IllegalArgumentException("bad input");

        assertThatThrownBy(() -> retry(5).retryOn(IOException.class).build()
                .execute(failingTimes(10, calls, failure))).isSameAs(failure);
        assertThat(calls).hasValue(1);
        assertThat(sleeps).isEmpty();
    }

    @Test
    void abortOnOverridesRetryableCondition() {
        AtomicInteger calls = new AtomicInteger();
        IllegalStateException failure = new IllegalStateException("fatal");

        assertThatThrownBy(() -> retry(5).abortOn(IllegalStateException.class).build()
                .execute(failingTimes(10, calls, failure))).isSameAs(failure);
        assertThat(calls).hasValue(1);
    }

    @Test
    void predicateCanInspectTheFailure() throws Exception {
        AtomicInteger calls = new AtomicInteger();

        String result = retry(3).retryOn(e -> e.getMessage().contains("503")).build()
                .execute(failingTimes(1, calls, new IOException("HTTP 503")));

        assertThat(result).isEqualTo("ok");
    }

    @Test
    void errorsAreNeverRetried() {
        AtomicInteger calls = new AtomicInteger();
        CheckedSupplier<String> action = () -> {
            calls.incrementAndGet();
            throw new AssertionError("not an Exception");
        };

        assertThatThrownBy(() -> retry(5).build().execute(action)).isInstanceOf(AssertionError.class);
        assertThat(calls).hasValue(1);
    }

    @Test
    void interruptionDuringBackoffStopsRetrying() {
        AtomicInteger calls = new AtomicInteger();
        Sleeper interrupted = duration -> {
            throw new InterruptedException("cancelled");
        };

        assertThatThrownBy(() -> retry(5).sleeper(interrupted).build()
                .execute(failingTimes(10, calls, new IOException())))
                .isInstanceOf(InterruptedException.class);
        assertThat(calls).hasValue(1);
    }

    @Test
    void interruptionInsideActionIsNotRetried() {
        AtomicInteger calls = new AtomicInteger();

        assertThatThrownBy(() -> retry(5).build().execute(failingTimes(10, calls, new InterruptedException())))
                .isInstanceOf(InterruptedException.class);
        assertThat(calls).hasValue(1);
    }

    @Test
    void listenerSeesEachRetryWithItsDelay() throws Exception {
        List<Retry.RetryEvent> events = new ArrayList<>();
        AtomicInteger calls = new AtomicInteger();

        retry(4).onRetry(events::add).build().execute(failingTimes(2, calls, new IOException("x")));

        assertThat(events).extracting(Retry.RetryEvent::failedAttempt).containsExactly(1, 2);
        assertThat(events).extracting(Retry.RetryEvent::delay)
                .containsExactly(Duration.ofMillis(100), Duration.ofMillis(200));
    }

    @Test
    void rejectsInvalidConfiguration() {
        assertThatThrownBy(() -> Retry.builder().maxAttempts(0)).isInstanceOf(IllegalArgumentException.class);
    }
}
