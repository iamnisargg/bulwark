package io.github.iamnisargg.bulwark.ratelimit;

import io.github.iamnisargg.bulwark.TimeSource;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Applies an independent {@link RateLimiter} to each key (user, API key, IP address, ...).
 *
 * <p>Limiters are created lazily and evicted once idle for {@code maxIdle}, which bounds memory under
 * high-cardinality keys. Eviction is amortised: whichever caller notices a sweep is due performs it, and a
 * CAS on the last-sweep timestamp guarantees only one thread does so. Choose {@code maxIdle} at least as
 * long as a limiter needs to fully recover; otherwise eviction hands the key a fresh burst.
 */
public final class KeyedRateLimiter<K> {

    private static final class Entry {
        final RateLimiter limiter;
        volatile long lastAccessNanos;

        Entry(RateLimiter limiter, long now) {
            this.limiter = limiter;
            this.lastAccessNanos = now;
        }
    }

    private final Function<? super K, ? extends RateLimiter> factory;
    private final long maxIdleNanos;
    private final TimeSource time;
    private final ConcurrentHashMap<K, Entry> entries = new ConcurrentHashMap<>();
    private final AtomicLong lastSweepNanos;

    public KeyedRateLimiter(Function<? super K, ? extends RateLimiter> factory, Duration maxIdle) {
        this(factory, maxIdle, TimeSource.system());
    }

    public KeyedRateLimiter(Function<? super K, ? extends RateLimiter> factory, Duration maxIdle, TimeSource time) {
        this.factory = Objects.requireNonNull(factory, "factory");
        if (Objects.requireNonNull(maxIdle, "maxIdle").isNegative() || maxIdle.isZero()) {
            throw new IllegalArgumentException("maxIdle must be positive");
        }
        this.maxIdleNanos = maxIdle.toNanos();
        this.time = Objects.requireNonNull(time, "time");
        this.lastSweepNanos = new AtomicLong(time.nanoTime());
    }

    public RateLimiter.Decision evaluate(K key, int permits) {
        Objects.requireNonNull(key, "key");
        long now = time.nanoTime();
        sweepIfDue(now);
        Entry entry = entries.computeIfAbsent(key, k -> new Entry(factory.apply(k), now));
        entry.lastAccessNanos = now;
        return entry.limiter.evaluate(permits);
    }

    public boolean tryAcquire(K key) {
        return evaluate(key, 1).allowed();
    }

    public boolean tryAcquire(K key, int permits) {
        return evaluate(key, permits).allowed();
    }

    /** Number of keys currently tracked. */
    public int trackedKeys() {
        return entries.size();
    }

    private void sweepIfDue(long now) {
        long last = lastSweepNanos.get();
        if (now - last >= maxIdleNanos && lastSweepNanos.compareAndSet(last, now)) {
            entries.values().removeIf(entry -> now - entry.lastAccessNanos >= maxIdleNanos);
        }
    }
}
