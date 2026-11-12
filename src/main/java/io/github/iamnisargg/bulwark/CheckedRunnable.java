package io.github.iamnisargg.bulwark;

/** A void action that may fail with a checked exception. */
@FunctionalInterface
public interface CheckedRunnable {

    void run() throws Exception;
}
