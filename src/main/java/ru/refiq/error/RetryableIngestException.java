package ru.refiq.error;

public class RetryableIngestException extends IngestException {

    public RetryableIngestException(String message) {
        super(message);
    }

    public RetryableIngestException(String message, Throwable cause) {
        super(message, cause);
    }
}
