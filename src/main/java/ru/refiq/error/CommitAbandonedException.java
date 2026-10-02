package ru.refiq.error;

public class CommitAbandonedException extends IngestException {

    public CommitAbandonedException(String message, Throwable cause) {
        super(message, cause);
    }
}
