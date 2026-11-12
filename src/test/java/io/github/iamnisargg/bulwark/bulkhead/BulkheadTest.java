package io.github.iamnisargg.bulwark.bulkhead;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class BulkheadTest {

    private final ExecutorService pool = Executors.newCachedThreadPool();

    @AfterEach
    void shutDown() {
        pool.shutdownNow();
    }

    /** Occupies {@code slots} permits until the returned latch is released. */
    private List<Future<Object>> occupy(Bulkhead bulkhead, int slots, CountDownLatch release) throws Exception {
        CountDownLatch entered = new CountDownLatch(slots);
        List<Future<Object>> holders = new ArrayList<>();
        for (int i = 0; i < slots; i++) {
            holders.add(pool.submit(() -> bulkhead.execute(() -> {
                entered.countDown();
                release.await();
                return null;
            })));
        }
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
        return holders;
    }

    @Test
    void neverRunsMoreThanMaxConcurrentCalls() throws Exception {
        int max = 3;
        Bulkhead bulkhead = Bulkhead.builder().maxConcurrentCalls(max).maxWait(Duration.ofSeconds(30)).build();
        AtomicInteger current = new AtomicInteger();
        AtomicInteger peak = new AtomicInteger();
        AtomicInteger completed = new AtomicInteger();
        List<Future<Object>> futures = new ArrayList<>();

        for (int i = 0; i < 60; i++) {
            futures.add(pool.submit(() -> bulkhead.execute(() -> {
                peak.accumulateAndGet(current.incrementAndGet(), Math::max);
                Thread.sleep(2);
                current.decrementAndGet();
                completed.incrementAndGet();
                return null;
            })));
        }
        for (Future<Object> future : futures) {
            future.get(30, TimeUnit.SECONDS);
        }

        assertThat(completed).hasValue(60);
        assertThat(peak.get()).isBetween(1, max);
        assertThat(bulkhead.availablePermits()).isEqualTo(max);
    }

    @Test
    void failsFastWhenFullAndNoWaitConfigured() throws Exception {
        Bulkhead bulkhead = Bulkhead.builder().name("db").maxConcurrentCalls(2).build();
        CountDownLatch release = new CountDownLatch(1);
        List<Future<Object>> holders = occupy(bulkhead, 2, release);

        assertThatThrownBy(() -> bulkhead.execute(() -> "x"))
                .isInstanceOfSatisfying(BulkheadFullException.class,
                        e -> assertThat(e.bulkheadName()).isEqualTo("db"));
        assertThat(bulkhead.concurrentCalls()).isEqualTo(2);
        assertThat(bulkhead.rejectedCalls()).isEqualTo(1);

        release.countDown();
        for (Future<Object> holder : holders) {
            holder.get(5, TimeUnit.SECONDS);
        }
        assertThat(bulkhead.availablePermits()).isEqualTo(2);
    }

    @Test
    void waitsForASlotWithinMaxWait() throws Exception {
        Bulkhead bulkhead = Bulkhead.builder().maxConcurrentCalls(1).maxWait(Duration.ofSeconds(10)).build();
        CountDownLatch release = new CountDownLatch(1);
        occupy(bulkhead, 1, release);

        Future<String> waiter = pool.submit(() -> bulkhead.execute(() -> "got in"));
        release.countDown();

        assertThat(waiter.get(5, TimeUnit.SECONDS)).isEqualTo("got in");
        assertThat(bulkhead.rejectedCalls()).isZero();
    }

    @Test
    void givesUpAfterMaxWait() throws Exception {
        Bulkhead bulkhead = Bulkhead.builder().maxConcurrentCalls(1).maxWait(Duration.ofMillis(50)).build();
        CountDownLatch release = new CountDownLatch(1);
        occupy(bulkhead, 1, release);

        long start = System.nanoTime();
        assertThatThrownBy(() -> bulkhead.execute(() -> "x")).isInstanceOf(BulkheadFullException.class);

        assertThat(Duration.ofNanos(System.nanoTime() - start)).isGreaterThanOrEqualTo(Duration.ofMillis(45));
        release.countDown();
    }

    @Test
    void releasesTheSlotWhenTheActionThrows() {
        Bulkhead bulkhead = Bulkhead.builder().maxConcurrentCalls(1).build();

        for (int i = 0; i < 3; i++) {
            assertThatThrownBy(() -> bulkhead.execute(() -> {
                throw new IOException("boom");
            })).isInstanceOf(IOException.class);
        }

        assertThat(bulkhead.availablePermits()).isEqualTo(1);
    }

    @Test
    void interruptedWaiterPropagatesAndLeavesNoLeakedSlot() throws Exception {
        Bulkhead bulkhead = Bulkhead.builder().maxConcurrentCalls(1).maxWait(Duration.ofSeconds(30)).build();
        CountDownLatch release = new CountDownLatch(1);
        occupy(bulkhead, 1, release);
        AtomicBoolean interrupted = new AtomicBoolean();
        Thread waiter = new Thread(() -> {
            try {
                bulkhead.execute(() -> "never");
            } catch (InterruptedException e) {
                interrupted.set(true);
            } catch (Exception e) {
                // any other outcome leaves the flag unset and fails the assertion below
            }
        });

        waiter.start();
        waiter.interrupt();
        waiter.join(5_000);

        assertThat(interrupted).isTrue();
        release.countDown();
        assertThat(waitUntil(() -> bulkhead.availablePermits() == 1)).isTrue();
    }

    @Test
    void rejectsInvalidConfiguration() {
        assertThatThrownBy(() -> Bulkhead.builder().maxConcurrentCalls(0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Bulkhead.builder().maxWait(Duration.ofMillis(-1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static boolean waitUntil(java.util.function.BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            Thread.sleep(5);
        }
        return condition.getAsBoolean();
    }
}
