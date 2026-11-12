package io.github.iamnisargg.bulwark;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/** Deterministic {@link TimeSource} for tests; time only moves when told to. */
public final class ManualTimeSource implements TimeSource {

    private final AtomicLong nanos = new AtomicLong();

    public ManualTimeSource() {
    }

    @Override
    public long nanoTime() {
        return nanos.get();
    }

    public void advance(Duration duration) {
        nanos.addAndGet(duration.toNanos());
    }

    public void advanceNanos(long delta) {
        nanos.addAndGet(delta);
    }
}
