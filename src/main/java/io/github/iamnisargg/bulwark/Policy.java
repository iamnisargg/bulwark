package io.github.iamnisargg.bulwark;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * A resilience policy that guards the execution of an action.
 *
 * <p>Policies compose: {@link #andThen(Policy)} and {@link #chain(Policy...)} build a pipeline in which
 * the first policy is the outermost wrapper. A typical ordering is
 * {@code retry -> circuit breaker -> rate limiter -> bulkhead -> action}, so that every retry attempt is
 * itself subject to the downstream protections.
 *
 * <p>Implementations must be thread-safe.
 */
public interface Policy {

    /**
     * Executes {@code action} under this policy.
     *
     * @throws Exception whatever the action throws, or a policy-specific rejection such as
     *                   {@link BulwarkException} subclasses
     */
    <T> T execute(CheckedSupplier<T> action) throws Exception;

    /** Executes a checked void action under this policy. */
    default void runChecked(CheckedRunnable action) throws Exception {
        Objects.requireNonNull(action, "action");
        execute(() -> {
            action.run();
            return null;
        });
    }

    /** Executes an unchecked action; checked failures raised by the policy itself are wrapped. */
    default <T> T get(Supplier<? extends T> action) {
        Objects.requireNonNull(action, "action");
        try {
            return execute(action::get);
        } catch (RuntimeException | Error e) {
            throw e;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new BulwarkException("Interrupted while executing guarded action", e);
        } catch (Exception e) {
            throw new BulwarkException("Guarded action failed", e);
        }
    }

    /** Executes an unchecked void action. */
    default void run(Runnable action) {
        Objects.requireNonNull(action, "action");
        get(() -> {
            action.run();
            return null;
        });
    }

    /** Returns a policy in which {@code this} wraps {@code inner}, which in turn wraps the action. */
    default Policy andThen(Policy inner) {
        Objects.requireNonNull(inner, "inner");
        Policy outer = this;
        return new Policy() {
            @Override
            public <T> T execute(CheckedSupplier<T> action) throws Exception {
                return outer.execute(() -> inner.execute(action));
            }
        };
    }

    /** Composes policies so that the first one is outermost. With no arguments, returns {@link #identity()}. */
    static Policy chain(Policy... policies) {
        Objects.requireNonNull(policies, "policies");
        Policy result = identity();
        for (int i = policies.length - 1; i >= 0; i--) {
            Policy current = Objects.requireNonNull(policies[i], "policy at index " + i);
            result = (i == policies.length - 1) ? current : current.andThen(result);
        }
        return result;
    }

    /** A policy that simply runs the action. */
    static Policy identity() {
        return new Policy() {
            @Override
            public <T> T execute(CheckedSupplier<T> action) throws Exception {
                return action.get();
            }
        };
    }
}
