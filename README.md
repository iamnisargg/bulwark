# Bulwark

[![CI](https://github.com/iamnisargg/bulwark/actions/workflows/ci.yml/badge.svg)](https://github.com/iamnisargg/bulwark/actions/workflows/ci.yml)
[![Java 17+](https://img.shields.io/badge/Java-17%2B-blue)](https://openjdk.org/projects/jdk/17/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

**A small, dependency-free resilience toolkit for Java 17+.** Bulwark protects calls to flaky or slow
dependencies with rate limiting, retries, circuit breaking and bulkheads, and lets you compose them into a
single policy.

- **Zero runtime dependencies**, shipped as a JPMS module (`io.github.iamnisargg.bulwark`).
- **Thread-safe by design**: lock-free where a single CAS is enough, a tiny lock only where it is not.
- **Deterministic tests**: every time-dependent component takes an injectable clock, so the suite never
  sleeps to check timing behaviour.

## Features

| Policy | What it does |
| --- | --- |
| `TokenBucketRateLimiter` | Bursts up to a capacity while enforcing a steady refill rate (GCRA, one atomic word of state). |
| `SlidingWindowRateLimiter` | Smooth "N per window" limit without the 2x boundary burst of a fixed window. |
| `KeyedRateLimiter` | Per-key limits (user, API key, IP) with lazy creation and idle eviction. |
| `Retry` | Exponential, fixed or no backoff with full or equal jitter; retry and abort predicates; listeners. |
| `CircuitBreaker` | CLOSED / OPEN / HALF_OPEN with a sliding failure-rate window, probe calls and metrics. |
| `Bulkhead` | Caps concurrent calls; fail fast or wait up to a deadline for a slot. |
| `Policy` | The common interface; policies compose with `andThen` / `Policy.chain`. |

## Quick start

Requires JDK 17+. Not yet published to Maven Central; build locally with `./mvnw install`.

```java
Policy resilient = Policy.chain(
        Retry.builder()
                .maxAttempts(3)
                .backoff(Backoff.exponential(Duration.ofMillis(100), 2.0, Duration.ofSeconds(5)).withFullJitter())
                // retrying an open breaker or a full bulkhead only adds load
                .abortOn(CircuitBreakerOpenException.class, RateLimitExceededException.class,
                        BulkheadFullException.class)
                .build(),
        CircuitBreaker.builder()
                .name("payments")
                .slidingWindowSize(20)
                .failureRateThreshold(0.5)
                .openDuration(Duration.ofSeconds(30))
                // local load shedding says nothing about the dependency's health
                .recordFailureIf(t -> !(t instanceof RateLimitExceededException
                        || t instanceof BulkheadFullException))
                .build(),
        TokenBucketRateLimiter.builder().capacity(50).refillRate(100, Duration.ofSeconds(1)).build().asPolicy(),
        Bulkhead.builder().maxConcurrentCalls(10).maxWait(Duration.ofMillis(200)).build());

String body = resilient.execute(() -> client.fetch(url));
```

The **first** policy in a chain is the **outermost**. The order above (retry, breaker, limiter, bulkhead)
means every retry attempt is itself rate limited and counted by the breaker.

Per-key limiting:

```java
KeyedRateLimiter<String> perUser = new KeyedRateLimiter<>(
        userId -> TokenBucketRateLimiter.builder().capacity(10).refillRate(10, Duration.ofSeconds(1)).build(),
        Duration.ofMinutes(10));

if (!perUser.tryAcquire("user-42")) {
    // reject with HTTP 429
}
```

### Try it

```bash
./mvnw package
java -cp target/classes examples/FlakyServiceDemo.java
```

The demo shows retries recovering from transient errors, a breaker tripping, failing fast and healing, a
rate limiter shedding a burst, and a bulkhead isolating slow calls.

## Design notes

**Token bucket as GCRA.** A classic token bucket stores a token count *and* a timestamp, two values that
must change atomically. Bulwark uses the mathematically equivalent Generic Cell Rate Algorithm, whose entire
state is one number, the "theoretical arrival time". Every decision is a single `compareAndSet` on an
`AtomicLong`: no locks, no allocation, no lost updates. Time is stored relative to construction, so
`System.nanoTime()` overflow is irrelevant.

**Sliding window counter.** Keeps counts for the current and previous fixed windows and estimates the
sliding total as `previous * (1 - elapsed) + current`. O(1) memory (an exact log is O(limit)) at the cost of
assuming even spread in the previous window. The state is an immutable record swapped by CAS, and the
`retryAfter` hint is computed analytically rather than by polling.

**Circuit breaker without lock contention.** A single lock guards only O(1) bookkeeping and is *never* held
while the guarded action runs or listeners fire. Each admitted call carries the breaker's *epoch*, which
increments on every transition; a call that finishes after the breaker has moved on is discarded instead of
corrupting the new state's statistics. OPEN becomes HALF_OPEN lazily on the first call after the wait
elapses, so there is no background thread to leak. In HALF_OPEN only a bounded number of probe calls are
admitted, and the breaker re-opens as soon as the failure rate crosses the threshold.

**Retry semantics that avoid surprises.** Only `Exception` is retried: `Error` and `InterruptedException`
propagate immediately. Non-retryable failures are rethrown unchanged; on exhaustion the *last* failure is the
cause of `RetryExhaustedException` and earlier ones are attached as suppressed exceptions.

**Bulkhead correctness.** Slots are released in a `finally`, so a throwing action never leaks one. A thread
interrupted while waiting leaves the semaphore untouched.

**Testability.** `TimeSource` and `Sleeper` are injected everywhere. The integration tests share one virtual
clock between the retry sleeper, the breaker and the limiter, so backoff delays advance the breaker's
open-window deterministically and the whole suite runs in about a second.

## Project layout

```
src/main/java/io/github/iamnisargg/bulwark/
  Policy, CheckedSupplier, CheckedRunnable, TimeSource, BulwarkException
  ratelimit/       TokenBucket, SlidingWindow, Keyed limiters
  retry/           Retry, Backoff, Sleeper
  circuitbreaker/  CircuitBreaker
  bulkhead/        Bulkhead
src/test/java/...  unit tests, 32-thread contention tests, policy-chain integration tests
examples/          runnable demo
```

## Build

```bash
./mvnw verify        # compile (javac -Xlint:all), run all tests, build the jar
```

CI runs the build on JDK 17 and 21 across Linux and Windows.

## License

[MIT](LICENSE)
