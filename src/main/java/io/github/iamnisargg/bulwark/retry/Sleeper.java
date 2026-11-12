package io.github.iamnisargg.bulwark.retry;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/** Abstraction over blocking waits so retry behaviour can be tested without real sleeping. */
@FunctionalInterface
public interface Sleeper {

    void sleep(Duration duration) throws InterruptedException;

    /** Blocks the calling thread. */
    static Sleeper system() {
        return duration -> {
            if (!duration.isZero() && !duration.isNegative()) {
                TimeUnit.NANOSECONDS.sleep(duration.toNanos());
            }
        };
    }
}
