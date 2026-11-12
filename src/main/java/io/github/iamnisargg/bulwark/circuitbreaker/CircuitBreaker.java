package io.github.iamnisargg.bulwark.circuitbreaker;

import io.github.iamnisargg.bulwark.CheckedSupplier;
import io.github.iamnisargg.bulwark.Policy;
import io.github.iamnisargg.bulwark.TimeSource;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Stops calling a failing dependency so it can recover, and fails fast in the meantime.
 *
 * <pre>
 *            failure rate &ge; threshold                 wait elapsed
 *   CLOSED -----------------------------&gt; OPEN -----------------------&gt; HALF_OPEN
 *      ^                                   ^                                |
 *      |    trial calls stay healthy         |   trial failures reach threshold
 *      +-----------------------------------+--------------------------------+
 * </pre>
 *
 * <ul>
 *   <li><b>CLOSED</b>: calls flow; the outcome of the last {@code slidingWindowSize} calls is tracked in a ring
 *       buffer. Once at least {@code minimumCalls} are recorded and the failure rate reaches the threshold,
 *       the breaker opens.</li>
 *   <li><b>OPEN</b>: calls are rejected with {@link CircuitBreakerOpenException} until {@code openDuration} has
 *       elapsed. The move to HALF_OPEN is lazy &mdash; it happens on the first call after the wait.</li>
 *   <li><b>HALF_OPEN</b>: only {@code halfOpenCalls} trial calls are admitted. The breaker re-opens as soon as
 *       the trial failures alone reach the threshold (no need to wait for slow stragglers), and closes once
 *       all trial calls have completed below it.</li>
 * </ul>
 *
 * <h2>Concurrency</h2>
 * State is guarded by a single lock held only for O(1) bookkeeping &mdash; never while the guarded action runs
 * or while listeners are notified. Every admitted call carries the <em>epoch</em> of the state it was admitted
 * in; the epoch advances on every transition, so a slow call that finishes after the circuit has moved on is
 * discarded instead of polluting the new state's statistics.
 *
 * <p>An exception for which the failure predicate returns {@code false} is treated as a success: the dependency
 * answered, so it is healthy.
 */
public final class CircuitBreaker implements Policy {

    private static final System.Logger LOG = System.getLogger(CircuitBreaker.class.getName());

    public enum State { CLOSED, OPEN, HALF_OPEN }

    public record StateChange(String breakerName, State from, State to) {
    }

    /** Point-in-time view of the breaker. */
    public record Metrics(State state, int bufferedCalls, int failedCalls, double failureRate, long notPermittedCalls) {
    }

    private final String name;
    private final double failureRateThreshold;
    private final int minimumCalls;
    private final long openNanos;
    private final int halfOpenCalls;
    private final Predicate<? super Throwable> failurePredicate;
    private final TimeSource time;
    private final Consumer<? super StateChange> listener;
    private final LongAdder notPermitted = new LongAdder();

    private final ReentrantLock lock = new ReentrantLock();
    // Everything below is guarded by lock.
    private final boolean[] window;
    private int windowCursor;
    private int windowCount;
    private int windowFailures;
    private State state = State.CLOSED;
    private long epoch;
    private long openedAtNanos;
    private int halfOpenPermitsLeft;
    private int halfOpenCompleted;
    private int halfOpenFailures;

    private CircuitBreaker(Builder builder) {
        this.name = builder.name;
        this.failureRateThreshold = builder.failureRateThreshold;
        this.window = new boolean[builder.slidingWindowSize];
        this.minimumCalls = builder.minimumCalls != 0
                ? builder.minimumCalls
                : Math.min(10, builder.slidingWindowSize);
        this.openNanos = builder.openDuration.toNanos();
        this.halfOpenCalls = builder.halfOpenCalls;
        this.failurePredicate = builder.failurePredicate;
        this.time = builder.time;
        this.listener = builder.listener;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public <T> T execute(CheckedSupplier<T> action) throws Exception {
        long permitEpoch = acquirePermission();
        T result;
        try {
            result = action.get();
        } catch (Throwable t) {
            record(permitEpoch, failurePredicate.test(t));
            throw t;
        }
        record(permitEpoch, false);
        return result;
    }

    public State state() {
        lock.lock();
        try {
            return state;
        } finally {
            lock.unlock();
        }
    }

    public Metrics metrics() {
        lock.lock();
        try {
            double rate = windowCount == 0 ? 0.0 : (double) windowFailures / windowCount;
            return new Metrics(state, windowCount, windowFailures, rate, notPermitted.sum());
        } finally {
            lock.unlock();
        }
    }

    /** Manually opens the breaker, e.g. while an operator drains a dependency. No-op if already open. */
    public void forceOpen() {
        moveTo(State.OPEN);
    }

    /** Manually closes the breaker and clears its statistics. No-op if already closed. */
    public void reset() {
        moveTo(State.CLOSED);
    }

    private void moveTo(State target) {
        StateChange change = null;
        lock.lock();
        try {
            if (state != target) {
                change = transitionTo(target);
            }
        } finally {
            lock.unlock();
        }
        publish(change);
    }

    private long acquirePermission() {
        StateChange change = null;
        boolean permitted;
        long permitEpoch;
        long retryAfterNanos = 0;
        lock.lock();
        try {
            if (state == State.OPEN && time.nanoTime() - openedAtNanos >= openNanos) {
                change = transitionTo(State.HALF_OPEN);
            }
            switch (state) {
                case CLOSED -> permitted = true;
                case HALF_OPEN -> {
                    permitted = halfOpenPermitsLeft > 0;
                    if (permitted) {
                        halfOpenPermitsLeft--;
                    }
                }
                default -> {
                    permitted = false;
                    retryAfterNanos = Math.max(0L, openNanos - (time.nanoTime() - openedAtNanos));
                }
            }
            permitEpoch = epoch;
            if (!permitted) {
                notPermitted.increment();
            }
        } finally {
            lock.unlock();
        }
        publish(change);
        if (!permitted) {
            throw new CircuitBreakerOpenException(name, Duration.ofNanos(retryAfterNanos));
        }
        return permitEpoch;
    }

    private void record(long permitEpoch, boolean failure) {
        StateChange change = null;
        lock.lock();
        try {
            if (permitEpoch != epoch) {
                return;
            }
            if (state == State.CLOSED) {
                pushResult(failure);
                if (windowCount >= minimumCalls && (double) windowFailures / windowCount >= failureRateThreshold) {
                    change = transitionTo(State.OPEN);
                }
            } else if (state == State.HALF_OPEN) {
                halfOpenCompleted++;
                if (failure) {
                    halfOpenFailures++;
                }
                if ((double) halfOpenFailures / halfOpenCalls >= failureRateThreshold) {
                    change = transitionTo(State.OPEN);
                } else if (halfOpenCompleted == halfOpenCalls) {
                    change = transitionTo(State.CLOSED);
                }
            }
        } finally {
            lock.unlock();
        }
        publish(change);
    }

    private void pushResult(boolean failure) {
        if (windowCount == window.length && window[windowCursor]) {
            windowFailures--;
        }
        window[windowCursor] = failure;
        if (failure) {
            windowFailures++;
        }
        windowCursor = (windowCursor + 1) % window.length;
        windowCount = Math.min(windowCount + 1, window.length);
    }

    /** Must be called with the lock held. */
    private StateChange transitionTo(State next) {
        State previous = state;
        state = next;
        epoch++;
        windowCursor = 0;
        windowCount = 0;
        windowFailures = 0;
        switch (next) {
            case OPEN -> openedAtNanos = time.nanoTime();
            case HALF_OPEN -> {
                halfOpenPermitsLeft = halfOpenCalls;
                halfOpenCompleted = 0;
                halfOpenFailures = 0;
            }
            case CLOSED -> {
            }
        }
        return new StateChange(name, previous, next);
    }

    private void publish(StateChange change) {
        if (change == null) {
            return;
        }
        try {
            listener.accept(change);
        } catch (RuntimeException e) {
            LOG.log(Level.WARNING, "Circuit breaker '" + name + "' state listener failed", e);
        }
    }

    public static final class Builder {

        private String name = "default";
        private double failureRateThreshold = 0.5;
        private int slidingWindowSize = 20;
        private int minimumCalls;
        private Duration openDuration = Duration.ofSeconds(30);
        private int halfOpenCalls = 3;
        private Predicate<? super Throwable> failurePredicate = t -> true;
        private TimeSource time = TimeSource.system();
        private Consumer<? super StateChange> listener = change -> {
        };

        private Builder() {
        }

        public Builder name(String name) {
            this.name = Objects.requireNonNull(name, "name");
            return this;
        }

        /** Failure ratio in {@code (0, 1]} at which the breaker opens. Defaults to 0.5. */
        public Builder failureRateThreshold(double threshold) {
            if (!(threshold > 0.0 && threshold <= 1.0)) {
                throw new IllegalArgumentException("failureRateThreshold must be in (0, 1] but was " + threshold);
            }
            this.failureRateThreshold = threshold;
            return this;
        }

        /** Number of most recent calls considered while CLOSED. Defaults to 20. */
        public Builder slidingWindowSize(int size) {
            if (size < 1) {
                throw new IllegalArgumentException("slidingWindowSize must be at least 1");
            }
            this.slidingWindowSize = size;
            return this;
        }

        /** Calls required in the window before the failure rate is evaluated. Defaults to min(10, window size). */
        public Builder minimumCalls(int minimumCalls) {
            if (minimumCalls < 1) {
                throw new IllegalArgumentException("minimumCalls must be at least 1");
            }
            this.minimumCalls = minimumCalls;
            return this;
        }

        public Builder openDuration(Duration duration) {
            if (Objects.requireNonNull(duration, "duration").isNegative() || duration.isZero()) {
                throw new IllegalArgumentException("openDuration must be positive");
            }
            this.openDuration = duration;
            return this;
        }

        /** Trial calls admitted while HALF_OPEN. Defaults to 3. */
        public Builder halfOpenCalls(int calls) {
            if (calls < 1) {
                throw new IllegalArgumentException("halfOpenCalls must be at least 1");
            }
            this.halfOpenCalls = calls;
            return this;
        }

        /** Decides which exceptions count as failures. Defaults to all throwables. */
        public Builder recordFailureIf(Predicate<? super Throwable> predicate) {
            this.failurePredicate = Objects.requireNonNull(predicate, "predicate");
            return this;
        }

        /** Invoked (outside any lock) after each state transition. Exceptions it throws are logged and ignored. */
        public Builder onStateChange(Consumer<? super StateChange> listener) {
            this.listener = Objects.requireNonNull(listener, "listener");
            return this;
        }

        public Builder timeSource(TimeSource time) {
            this.time = Objects.requireNonNull(time, "time");
            return this;
        }

        public CircuitBreaker build() {
            if (minimumCalls > slidingWindowSize) {
                throw new IllegalStateException("minimumCalls (" + minimumCalls
                        + ") must not exceed slidingWindowSize (" + slidingWindowSize + ")");
            }
            return new CircuitBreaker(this);
        }
    }
}
