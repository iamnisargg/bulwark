package io.github.iamnisargg.bulwark.ratelimit;

import io.github.iamnisargg.bulwark.TimeSource;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Token-bucket limiter: a bucket holds up to {@code capacity} permits and is refilled continuously at a
 * fixed rate, allowing bursts up to the capacity while enforcing the long-run average rate.
 *
 * <h2>Implementation</h2>
 * Rather than storing a token count and a timestamp (two words that must change atomically), this class
 * uses the Generic Cell Rate Algorithm, which is mathematically equivalent: the whole state is a single
 * "theoretical arrival time" (TAT) &mdash; the instant at which the bucket would be completely full again.
 * A request for {@code n} permits is admitted iff {@code max(TAT, now) + n * interval - capacity * interval <= now},
 * and admission simply advances the TAT. That makes every decision one compare-and-set on an
 * {@link AtomicLong}: lock-free, allocation-free and immune to lost updates.
 *
 * <p>Time is tracked as an offset from construction so that {@link System#nanoTime()} overflow and
 * negative readings are irrelevant. The refill interval is {@code period / permits} rounded down to a whole
 * nanosecond.
 */
public final class TokenBucketRateLimiter implements RateLimiter {

    private final long capacity;
    private final long intervalNanos;
    private final long burstNanos;
    private final TimeSource time;
    private final long origin;
    private final AtomicLong tat = new AtomicLong();

    private TokenBucketRateLimiter(Builder builder) {
        this.capacity = builder.capacity != 0 ? builder.capacity : builder.refillPermits;
        this.intervalNanos = builder.refillPeriod.toNanos() / builder.refillPermits;
        if (intervalNanos < 1) {
            throw new IllegalArgumentException("refill rate is too high: more than one permit per nanosecond");
        }
        this.burstNanos = Math.multiplyExact(capacity, intervalNanos);
        this.time = builder.time;
        this.origin = time.nanoTime();
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public Decision evaluate(int permits) {
        if (permits <= 0 || permits > capacity) {
            throw new IllegalArgumentException("permits must be in [1, " + capacity + "] but was " + permits);
        }
        long increment = permits * intervalNanos;
        while (true) {
            long now = time.nanoTime() - origin;
            long current = tat.get();
            long newTat = Math.max(current, now) + increment;
            long admittedAt = newTat - burstNanos;
            if (admittedAt > now) {
                return Decision.rejected(admittedAt - now);
            }
            if (tat.compareAndSet(current, newTat)) {
                return Decision.ALLOWED;
            }
        }
    }

    /** A point-in-time estimate of how many permits could be acquired right now. */
    public long availablePermits() {
        long now = time.nanoTime() - origin;
        long deficit = Math.max(tat.get(), now) - now;
        long available = capacity - (deficit + intervalNanos - 1) / intervalNanos;
        return Math.max(0L, Math.min(capacity, available));
    }

    public long capacity() {
        return capacity;
    }

    public static final class Builder {

        private long capacity;
        private long refillPermits;
        private Duration refillPeriod;
        private TimeSource time = TimeSource.system();

        private Builder() {
        }

        /** Maximum burst size. Defaults to the number of permits refilled per period. */
        public Builder capacity(long capacity) {
            if (capacity <= 0) {
                throw new IllegalArgumentException("capacity must be positive");
            }
            this.capacity = capacity;
            return this;
        }

        /** Sustained rate: {@code permits} become available every {@code period}. */
        public Builder refillRate(long permits, Duration period) {
            if (permits <= 0) {
                throw new IllegalArgumentException("permits must be positive");
            }
            if (Objects.requireNonNull(period, "period").isNegative() || period.isZero()) {
                throw new IllegalArgumentException("period must be positive");
            }
            this.refillPermits = permits;
            this.refillPeriod = period;
            return this;
        }

        public Builder timeSource(TimeSource time) {
            this.time = Objects.requireNonNull(time, "time");
            return this;
        }

        public TokenBucketRateLimiter build() {
            if (refillPermits == 0) {
                throw new IllegalStateException("refillRate(...) is required");
            }
            return new TokenBucketRateLimiter(this);
        }
    }
}
