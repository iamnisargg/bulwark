package io.github.iamnisargg.bulwark;

/**
 * Source of monotonic time. Every time-dependent component takes a {@code TimeSource} so that
 * behaviour can be tested deterministically without sleeping.
 */
@FunctionalInterface
public interface TimeSource {

    /** Returns a monotonic reading in nanoseconds; only differences between readings are meaningful. */
    long nanoTime();

    /** The JVM's monotonic clock. */
    static TimeSource system() {
        return System::nanoTime;
    }
}
