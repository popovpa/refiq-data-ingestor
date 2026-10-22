package ru.refiq.config;

public interface IngestStorageStartupValidator {

    boolean supports(String type);

    void validate(DataIngestProperties properties);
}
