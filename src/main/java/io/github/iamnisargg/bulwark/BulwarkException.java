package io.github.iamnisargg.bulwark;

/** Base type for all unchecked exceptions raised by Bulwark itself (as opposed to the guarded action). */
public class BulwarkException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public BulwarkException(String message) {
        super(message);
    }

    public BulwarkException(String message, Throwable cause) {
        super(message, cause);
    }
}
