package io.github.iamnisargg.bulwark.circuitbreaker;

import io.github.iamnisargg.bulwark.BulwarkException;
import java.time.Duration;

/** Raised when a call is rejected because the circuit breaker is open (or its half-open quota is used up). */
public final class CircuitBreakerOpenException extends BulwarkException {

    private static final long serialVersionUID = 1L;

    private final String breakerName;
    private final transient Duration retryAfter;

    CircuitBreakerOpenException(String breakerName, Duration retryAfter) {
        super("Circuit breaker '" + breakerName + "' is not accepting calls; retry after "
                + retryAfter.toMillis() + " ms");
        this.breakerName = breakerName;
        this.retryAfter = retryAfter;
    }

    public String breakerName() {
        return breakerName;
    }

    /** Remaining time until the breaker will admit trial calls; zero if it is already waiting on trial calls. */
    public Duration retryAfter() {
        return retryAfter;
    }
}
