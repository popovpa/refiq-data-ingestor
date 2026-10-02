package ru.refiq.error;

public class FatalIngestException extends IngestException {

    public FatalIngestException(String message) {
        super(message);
    }

    public FatalIngestException(String message, Throwable cause) {
        super(message, cause);
    }
}
