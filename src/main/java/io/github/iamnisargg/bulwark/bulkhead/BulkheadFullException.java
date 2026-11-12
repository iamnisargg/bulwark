package io.github.iamnisargg.bulwark.bulkhead;

import io.github.iamnisargg.bulwark.BulwarkException;

/** Raised when the bulkhead has no free slot and the caller's maximum wait elapsed. */
public final class BulkheadFullException extends BulwarkException {

    private static final long serialVersionUID = 1L;

    private final String bulkheadName;

    BulkheadFullException(String bulkheadName, int maxConcurrentCalls) {
        super("Bulkhead '" + bulkheadName + "' is full (" + maxConcurrentCalls + " concurrent calls)");
        this.bulkheadName = bulkheadName;
    }

    public String bulkheadName() {
        return bulkheadName;
    }
}
