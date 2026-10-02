package ru.refiq.strategy;

import ru.refiq.batch.IngestBatch;

public interface IngestStrategy {

    String type();

    void process(IngestBatch batch);
}
