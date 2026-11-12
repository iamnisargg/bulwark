package io.github.iamnisargg.bulwark.retry;

import io.github.iamnisargg.bulwark.CheckedSupplier;
import io.github.iamnisargg.bulwark.Policy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Re-runs a failing action with a configurable {@link Backoff}.
 *
 * <ul>
 *   <li>Only {@link Exception}s are retried; {@link Error}s always propagate immediately.</li>
 *   <li>{@link InterruptedException} is never retried: it is rethrown at once so cancellation is honoured.</li>
 *   <li>A non-retryable failure is rethrown unchanged; running out of attempts raises
 *       {@link RetryExhaustedException}.</li>
 * </ul>
 */
public final class Retry implements Policy {

    /** Emitted just before sleeping ahead of a retry. */
    public record RetryEvent(int failedAttempt, Exception failure, Duration delay) {
    }

    private final int maxAttempts;
    private final Backoff backoff;
    private final Predicate<? super Exception> retryable;
    private final Sleeper sleeper;
    private final Consumer<? super RetryEvent> listener;

    private Retry(Builder builder) {
        this.maxAttempts = builder.maxAttempts;
        this.backoff = builder.backoff;
        this.retryable = builder.retryable;
        this.sleeper = builder.sleeper;
        this.listener = builder.listener;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public <T> T execute(CheckedSupplier<T> action) throws Exception {
        List<Exception> earlier = new ArrayList<>();
        for (int attempt = 1; ; attempt++) {
            try {
                return action.get();
            } catch (InterruptedException e) {
                throw e;
            } catch (Exception e) {
                if (!retryable.test(e)) {
                    throw e;
                }
                if (attempt >= maxAttempts) {
                    throw new RetryExhaustedException(attempt, e, earlier);
                }
                earlier.add(e);
                Duration delay = backoff.delay(attempt);
                listener.accept(new RetryEvent(attempt, e, delay));
                sleeper.sleep(delay);
            }
        }
    }

    public static final class Builder {

        private int maxAttempts = 3;
        private Backoff backoff = Backoff.exponential(Duration.ofMillis(100), 2.0, Duration.ofSeconds(10))
                .withFullJitter();
        private Predicate<? super Exception> retryable = e -> true;
        private Sleeper sleeper = Sleeper.system();
        private Consumer<? super RetryEvent> listener = event -> {
        };

        private Builder() {
        }

        /** Total number of attempts including the first one. Defaults to 3. */
        public Builder maxAttempts(int maxAttempts) {
            if (maxAttempts < 1) {
                throw new IllegalArgumentException("maxAttempts must be at least 1");
            }
            this.maxAttempts = maxAttempts;
            return this;
        }

        public Builder backoff(Backoff backoff) {
            this.backoff = Objects.requireNonNull(backoff, "backoff");
            return this;
        }

        /** Retry only failures matching the predicate. Replaces any previous retry condition. */
        public Builder retryOn(Predicate<? super Exception> retryable) {
            this.retryable = Objects.requireNonNull(retryable, "retryable");
            return this;
        }

        /** Retry only failures that are instances of one of {@code types}. */
        @SafeVarargs
        public final Builder retryOn(Class<? extends Exception>... types) {
            List<Class<? extends Exception>> accepted = new ArrayList<>(types.length);
            for (Class<? extends Exception> type : types) {
                accepted.add(type);
            }
            return retryOn(e -> accepted.stream().anyMatch(type -> type.isInstance(e)));
        }

        /** Never retry failures that are instances of one of {@code types}, even if otherwise retryable. */
        @SafeVarargs
        public final Builder abortOn(Class<? extends Exception>... types) {
            List<Class<? extends Exception>> rejected = new ArrayList<>(types.length);
            for (Class<? extends Exception> type : types) {
                rejected.add(type);
            }
            Predicate<? super Exception> previous = this.retryable;
            this.retryable = e -> previous.test(e) && rejected.stream().noneMatch(type -> type.isInstance(e));
            return this;
        }

        public Builder onRetry(Consumer<? super RetryEvent> listener) {
            this.listener = Objects.requireNonNull(listener, "listener");
            return this;
        }

        public Builder sleeper(Sleeper sleeper) {
            this.sleeper = Objects.requireNonNull(sleeper, "sleeper");
            return this;
        }

        public Retry build() {
            return new Retry(this);
        }
    }
}
