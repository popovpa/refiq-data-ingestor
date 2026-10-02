package ru.refiq.strategy.clickstream.enrichment;

import org.springframework.stereotype.Component;
import ru.refiq.error.FatalIngestException;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Process-local temporary ids. Not unique across instances and not a distributed scheme.
 * Replaced when a trusted upstream event id is available.
 */
@Component
public class TimeBasedEventIdGenerator implements EventIdGenerator {

    private final AtomicLong sequence = new AtomicLong(System.currentTimeMillis() * 1_000L);

    @Override
    public long nextId() {
        long id = sequence.incrementAndGet();
        if (id <= 0L) {
            throw new FatalIngestException("event id space exhausted");
        }
        return id;
    }
}
