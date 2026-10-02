package ru.refiq.error;

public class InvalidRecordException extends IngestException {

    public InvalidRecordException(String message) {
        super(message);
    }
}
