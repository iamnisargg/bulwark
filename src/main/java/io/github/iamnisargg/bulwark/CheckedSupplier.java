package io.github.iamnisargg.bulwark;

/** A value-producing action that may fail with a checked exception. */
@FunctionalInterface
public interface CheckedSupplier<T> {

    T get() throws Exception;
}
