package io.github.iamnisargg.bulwark.ratelimit;

import io.github.iamnisargg.bulwark.BulwarkException;
import java.time.Duration;

/** Raised by {@link RateLimiter#asPolicy()} when a call is rejected. */
public final class RateLimitExceededException extends BulwarkException {

    private static final long serialVersionUID = 1L;

    private final transient Duration retryAfter;

    public RateLimitExceededException(Duration retryAfter) {
        super("Rate limit exceeded; retry after " + retryAfter.toMillis() + " ms");
        this.retryAfter = retryAfter;
    }

    /** Suggested delay before retrying, suitable for an HTTP {@code Retry-After} header. */
    public Duration retryAfter() {
        return retryAfter;
    }
}
