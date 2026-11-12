package io.github.iamnisargg.bulwark.retry;

import io.github.iamnisargg.bulwark.BulwarkException;
import java.util.List;

/**
 * All attempts failed. The last failure is the {@linkplain #getCause() cause}; earlier failures are attached
 * as {@linkplain #getSuppressed() suppressed} exceptions in the order they occurred.
 */
public final class RetryExhaustedException extends BulwarkException {

    private static final long serialVersionUID = 1L;

    private final int attempts;

    RetryExhaustedException(int attempts, Exception last, List<Exception> earlier) {
        super("Gave up after " + attempts + " attempts: " + last, last);
        this.attempts = attempts;
        earlier.forEach(this::addSuppressed);
    }

    public int attempts() {
        return attempts;
    }
}
