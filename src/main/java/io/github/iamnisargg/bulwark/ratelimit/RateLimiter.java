package io.github.iamnisargg.bulwark.ratelimit;

import io.github.iamnisargg.bulwark.CheckedSupplier;
import io.github.iamnisargg.bulwark.Policy;
import java.time.Duration;

/**
 * Decides whether work may proceed right now. Implementations are thread-safe and never block.
 */
public interface RateLimiter {

    /**
     * Attempts to take {@code permits} permits.
     *
     * @throws IllegalArgumentException if {@code permits} is not positive or exceeds what the limiter
     *                                  could ever grant at once
     */
    Decision evaluate(int permits);

    default boolean tryAcquire(int permits) {
        return evaluate(permits).allowed();
    }

    default boolean tryAcquire() {
        return tryAcquire(1);
    }

    /** Adapts this limiter to a {@link Policy} that fails fast with {@link RateLimitExceededException}. */
    default Policy asPolicy() {
        RateLimiter limiter = this;
        return new Policy() {
            @Override
            public <T> T execute(CheckedSupplier<T> action) throws Exception {
                Decision decision = limiter.evaluate(1);
                if (!decision.allowed()) {
                    throw new RateLimitExceededException(decision.retryAfter());
                }
                return action.get();
            }
        };
    }

    /**
     * Outcome of a rate-limit check.
     *
     * @param allowed         whether the permits were granted
     * @param retryAfterNanos for rejections, how long until the same request would be admitted assuming no
     *                        other traffic; always {@code 0} when allowed
     */
    record Decision(boolean allowed, long retryAfterNanos) {

        public static final Decision ALLOWED = new Decision(true, 0L);

        public Decision {
            if (retryAfterNanos < 0 || (allowed && retryAfterNanos != 0)) {
                throw new IllegalArgumentException("invalid decision: allowed=" + allowed
                        + ", retryAfterNanos=" + retryAfterNanos);
            }
        }

        public static Decision rejected(long retryAfterNanos) {
            return new Decision(false, Math.max(1L, retryAfterNanos));
        }

        public Duration retryAfter() {
            return Duration.ofNanos(retryAfterNanos);
        }
    }
}
