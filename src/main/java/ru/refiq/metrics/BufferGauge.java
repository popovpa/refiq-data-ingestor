package ru.refiq.metrics;

import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class BufferGauge {

    private final AtomicInteger records = new AtomicInteger();
    private final AtomicLong bytes = new AtomicLong();

    public void set(int records, long bytes) {
        this.records.set(records);
        this.bytes.set(bytes);
    }

    public int records() {
        return records.get();
    }

    public long bytes() {
        return bytes.get();
    }
}
