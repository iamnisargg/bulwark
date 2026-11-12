import io.github.iamnisargg.bulwark.Policy;
import io.github.iamnisargg.bulwark.bulkhead.Bulkhead;
import io.github.iamnisargg.bulwark.bulkhead.BulkheadFullException;
import io.github.iamnisargg.bulwark.circuitbreaker.CircuitBreaker;
import io.github.iamnisargg.bulwark.circuitbreaker.CircuitBreakerOpenException;
import io.github.iamnisargg.bulwark.ratelimit.RateLimiter;
import io.github.iamnisargg.bulwark.ratelimit.TokenBucketRateLimiter;
import io.github.iamnisargg.bulwark.retry.Backoff;
import io.github.iamnisargg.bulwark.retry.Retry;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Walks through each Bulwark policy against a simulated unreliable dependency.
 *
 * <pre>
 * ./mvnw package
 * java -cp target/classes examples/FlakyServiceDemo.java
 * </pre>
 */
public class FlakyServiceDemo {

    public static void main(String[] args) throws Exception {
        retryRecoversFromTransientErrors();
        circuitBreakerFailsFastThenHeals();
        rateLimiterShedsBursts();
        bulkheadIsolatesSlowCalls();
    }

    private static void retryRecoversFromTransientErrors() throws Exception {
        heading("Retry with exponential backoff and jitter");
        AtomicInteger attempts = new AtomicInteger();
        Retry retry = Retry.builder()
                .maxAttempts(5)
                .backoff(Backoff.exponential(Duration.ofMillis(50), 2.0, Duration.ofSeconds(1)).withFullJitter())
                .onRetry(e -> System.out.printf("  attempt %d failed (%s), retrying in %d ms%n",
                        e.failedAttempt(), e.failure().getMessage(), e.delay().toMillis()))
                .build();

        String result = retry.execute(() -> {
            if (attempts.incrementAndGet() < 4) {
                throw new IOException("connection reset");
            }
            return "200 OK";
        });
        System.out.println("  -> " + result + " after " + attempts + " attempts");
    }

    private static void circuitBreakerFailsFastThenHeals() throws Exception {
        heading("Circuit breaker: trip, fail fast, probe, recover");
        AtomicBoolean healthy = new AtomicBoolean(false);
        AtomicInteger reachedService = new AtomicInteger();
        CircuitBreaker breaker = CircuitBreaker.builder()
                .name("payments")
                .slidingWindowSize(4)
                .failureRateThreshold(0.5)
                .openDuration(Duration.ofMillis(500))
                .halfOpenCalls(1)
                .onStateChange(c -> System.out.println("  [state] " + c.from() + " -> " + c.to()))
                .build();

        for (int i = 1; i <= 8; i++) {
            try {
                breaker.execute(() -> {
                    reachedService.incrementAndGet();
                    if (!healthy.get()) {
                        throw new IOException("503");
                    }
                    return "ok";
                });
            } catch (CircuitBreakerOpenException e) {
                System.out.println("  call " + i + ": rejected without touching the service");
            } catch (IOException e) {
                System.out.println("  call " + i + ": service error " + e.getMessage());
            }
        }
        System.out.println("  only " + reachedService + " of 8 calls reached the failing service");

        healthy.set(true);
        Thread.sleep(600);
        System.out.println("  service recovered, probing: " + breaker.execute(() -> "ok")
                + " (breaker " + breaker.state() + ")");
    }

    private static void rateLimiterShedsBursts() {
        heading("Token bucket rate limiter (burst 5, refill 5/s)");
        RateLimiter limiter = TokenBucketRateLimiter.builder()
                .capacity(5)
                .refillRate(5, Duration.ofSeconds(1))
                .build();

        for (int i = 1; i <= 8; i++) {
            RateLimiter.Decision decision = limiter.evaluate(1);
            System.out.println("  request " + i + ": " + (decision.allowed()
                    ? "allowed"
                    : "rejected, retry in " + TimeUnit.NANOSECONDS.toMillis(decision.retryAfterNanos()) + " ms"));
        }
    }

    private static void bulkheadIsolatesSlowCalls() throws Exception {
        heading("Bulkhead: 2 slots, 6 concurrent callers hitting a slow dependency");
        Bulkhead bulkhead = Bulkhead.builder().name("reports").maxConcurrentCalls(2).build();
        Policy policy = Policy.chain(bulkhead);
        AtomicInteger served = new AtomicInteger();
        AtomicInteger shed = new AtomicInteger();

        ExecutorService pool = Executors.newFixedThreadPool(6);
        for (int i = 0; i < 6; i++) {
            pool.submit(() -> {
                try {
                    policy.execute(() -> {
                        Thread.sleep(300);
                        return served.incrementAndGet();
                    });
                } catch (BulkheadFullException e) {
                    shed.incrementAndGet();
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            });
        }
        pool.shutdown();
        pool.awaitTermination(5, TimeUnit.SECONDS);
        System.out.println("  served " + served + ", shed " + shed + " (slots free again: "
                + bulkhead.availablePermits() + ")");
    }

    private static void heading(String text) {
        System.out.println();
        System.out.println("== " + text);
    }
}
