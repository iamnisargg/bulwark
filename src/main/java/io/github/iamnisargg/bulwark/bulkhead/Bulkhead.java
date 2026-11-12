package io.github.iamnisargg.bulwark.bulkhead;

import io.github.iamnisargg.bulwark.CheckedSupplier;
import io.github.iamnisargg.bulwark.Policy;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * Caps how many calls may run concurrently against a resource, so one slow dependency cannot absorb every
 * thread in the application (the namesake of ship bulkheads that contain flooding to one compartment).
 *
 * <p>Callers that find the bulkhead full either fail immediately or wait up to {@code maxWait} for a slot.
 * Slots are released in a {@code finally} block, so a throwing action never leaks one, and a thread
 * interrupted while waiting leaves the bulkhead untouched.
 */
public final class Bulkhead implements Policy {

    private final String name;
    private final int maxConcurrentCalls;
    private final long maxWaitNanos;
    private final Semaphore semaphore;
    private final LongAdder rejected = new LongAdder();

    private Bulkhead(Builder builder) {
        this.name = builder.name;
        this.maxConcurrentCalls = builder.maxConcurrentCalls;
        this.maxWaitNanos = builder.maxWait.toNanos();
        this.semaphore = new Semaphore(maxConcurrentCalls, builder.fair);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public <T> T execute(CheckedSupplier<T> action) throws Exception {
        acquireSlot();
        try {
            return action.get();
        } finally {
            semaphore.release();
        }
    }

    private void acquireSlot() throws InterruptedException {
        if (semaphore.tryAcquire()) {
            return;
        }
        if (maxWaitNanos > 0 && semaphore.tryAcquire(maxWaitNanos, TimeUnit.NANOSECONDS)) {
            return;
        }
        rejected.increment();
        throw new BulkheadFullException(name, maxConcurrentCalls);
    }

    public int availablePermits() {
        return semaphore.availablePermits();
    }

    public int concurrentCalls() {
        return maxConcurrentCalls - semaphore.availablePermits();
    }

    /** Total number of calls rejected because no slot became available. */
    public long rejectedCalls() {
        return rejected.sum();
    }

    public static final class Builder {

        private String name = "default";
        private int maxConcurrentCalls = 25;
        private Duration maxWait = Duration.ZERO;
        private boolean fair;

        private Builder() {
        }

        public Builder name(String name) {
            this.name = Objects.requireNonNull(name, "name");
            return this;
        }

        /** Maximum calls allowed to run at once. Defaults to 25. */
        public Builder maxConcurrentCalls(int maxConcurrentCalls) {
            if (maxConcurrentCalls < 1) {
                throw new IllegalArgumentException("maxConcurrentCalls must be at least 1");
            }
            this.maxConcurrentCalls = maxConcurrentCalls;
            return this;
        }

        /** How long a caller may wait for a free slot. Defaults to zero (fail fast). */
        public Builder maxWait(Duration maxWait) {
            if (Objects.requireNonNull(maxWait, "maxWait").isNegative()) {
                throw new IllegalArgumentException("maxWait must not be negative");
            }
            this.maxWait = maxWait;
            return this;
        }

        /** Grant waiting callers slots in FIFO order. Costs some throughput; defaults to {@code false}. */
        public Builder fair(boolean fair) {
            this.fair = fair;
            return this;
        }

        public Bulkhead build() {
            return new Bulkhead(this);
        }
    }
}
