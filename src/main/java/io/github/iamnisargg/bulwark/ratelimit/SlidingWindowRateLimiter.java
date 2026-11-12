package io.github.iamnisargg.bulwark.ratelimit;

import io.github.iamnisargg.bulwark.TimeSource;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Sliding-window-counter limiter: at most {@code limit} permits per {@code window}, smoothed across window
 * boundaries.
 *
 * <p>A fixed-window counter lets a client send {@code 2 * limit} requests around a boundary. This limiter
 * keeps the counts of the current and previous fixed windows and estimates the sliding total as
 * {@code previous * (1 - elapsedFraction) + current}, the same technique used by large API gateways. It needs
 * O(1) memory per limiter (versus O(limit) for an exact request log) at the price of assuming requests in
 * the previous window were evenly spread.
 *
 * <p>State is an immutable record swapped with compare-and-set, so the class is lock-free.
 */
public final class SlidingWindowRateLimiter implements RateLimiter {

    private record Window(long index, long previous, long current) {
    }

    private final long limit;
    private final long windowNanos;
    private final TimeSource time;
    private final long origin;
    private final AtomicReference<Window> state = new AtomicReference<>(new Window(0, 0, 0));

    public SlidingWindowRateLimiter(long limit, Duration window) {
        this(limit, window, TimeSource.system());
    }

    public SlidingWindowRateLimiter(long limit, Duration window, TimeSource time) {
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive");
        }
        if (Objects.requireNonNull(window, "window").isNegative() || window.isZero()) {
            throw new IllegalArgumentException("window must be positive");
        }
        this.limit = limit;
        this.windowNanos = window.toNanos();
        this.time = Objects.requireNonNull(time, "time");
        this.origin = time.nanoTime();
    }

    @Override
    public Decision evaluate(int permits) {
        if (permits <= 0 || permits > limit) {
            throw new IllegalArgumentException("permits must be in [1, " + limit + "] but was " + permits);
        }
        while (true) {
            long now = time.nanoTime() - origin;
            Window observed = state.get();
            long index = Math.max(now / windowNanos, observed.index());
            Window rolled = roll(observed, index);
            long elapsed = Math.max(0L, now - index * windowNanos);

            double estimated = rolled.previous() * (double) (windowNanos - elapsed) / windowNanos + rolled.current();
            if (estimated + permits > limit) {
                return Decision.rejected(retryAfterNanos(rolled, permits, elapsed));
            }
            Window next = new Window(index, rolled.previous(), rolled.current() + permits);
            if (state.compareAndSet(observed, next)) {
                return Decision.ALLOWED;
            }
        }
    }

    private static Window roll(Window window, long index) {
        long gap = index - window.index();
        if (gap == 0) {
            return window;
        }
        return gap == 1 ? new Window(index, window.current(), 0) : new Window(index, 0, 0);
    }

    /** Solves for the earliest instant the request would fit, assuming no further traffic. */
    private long retryAfterNanos(Window w, int permits, long elapsed) {
        double wait;
        if (w.current() + permits <= limit) {
            double slack = limit - w.current() - permits;
            wait = windowNanos * (1.0 - slack / w.previous()) - elapsed;
        } else {
            double slack = limit - permits;
            wait = (windowNanos - elapsed) + windowNanos * (1.0 - slack / w.current());
        }
        return (long) Math.ceil(wait);
    }
}
