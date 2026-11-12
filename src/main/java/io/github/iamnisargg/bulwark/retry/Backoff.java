package io.github.iamnisargg.bulwark.retry;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.random.RandomGenerator;

/** Computes how long to wait before the next retry. */
@FunctionalInterface
public interface Backoff {

    /**
     * Delay before retry number {@code retryNumber}, where {@code 1} is the first retry (i.e. the wait after
     * the first failed attempt).
     */
    Duration delay(int retryNumber);

    /** Retry immediately. */
    static Backoff none() {
        return retryNumber -> Duration.ZERO;
    }

    static Backoff fixed(Duration delay) {
        requireNonNegative(delay, "delay");
        return retryNumber -> delay;
    }

    /**
     * Exponentially growing delay: {@code initial * multiplier^(retryNumber - 1)}, capped at {@code max}. The
     * computation is done in floating point and clamped, so very large retry numbers cannot overflow.
     */
    static Backoff exponential(Duration initial, double multiplier, Duration max) {
        requireNonNegative(initial, "initial");
        requireNonNegative(max, "max");
        if (!(multiplier >= 1.0) || Double.isInfinite(multiplier)) {
            throw new IllegalArgumentException("multiplier must be finite and >= 1 but was " + multiplier);
        }
        if (initial.compareTo(max) > 0) {
            throw new IllegalArgumentException("initial must not exceed max");
        }
        double initialNanos = initial.toNanos();
        double maxNanos = max.toNanos();
        return retryNumber -> {
            double nanos = initialNanos * Math.pow(multiplier, Math.max(0, retryNumber - 1));
            return Duration.ofNanos((long) Math.min(nanos, maxNanos));
        };
    }

    /**
     * Full jitter: wait a uniformly random time in {@code [0, delay)}. This de-synchronises clients that fail
     * together and gives the best contention reduction at the cost of occasionally retrying almost instantly.
     */
    default Backoff withFullJitter() {
        return withFullJitter(ThreadLocalRandom.current());
    }

    default Backoff withFullJitter(RandomGenerator random) {
        Objects.requireNonNull(random, "random");
        return retryNumber -> {
            long bound = delay(retryNumber).toNanos();
            return bound == 0 ? Duration.ZERO : Duration.ofNanos(random.nextLong(bound));
        };
    }

    /** Equal jitter: half the delay is fixed, the other half is uniformly random, guaranteeing a minimum wait. */
    default Backoff withEqualJitter() {
        return withEqualJitter(ThreadLocalRandom.current());
    }

    default Backoff withEqualJitter(RandomGenerator random) {
        Objects.requireNonNull(random, "random");
        return retryNumber -> {
            long nanos = delay(retryNumber).toNanos();
            if (nanos == 0) {
                return Duration.ZERO;
            }
            long half = nanos / 2;
            return Duration.ofNanos(half + random.nextLong(nanos - half));
        };
    }

    private static void requireNonNegative(Duration duration, String name) {
        if (Objects.requireNonNull(duration, name).isNegative()) {
            throw new IllegalArgumentException(name + " must not be negative");
        }
    }
}
