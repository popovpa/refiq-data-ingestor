package ru.refiq.consumer;

import java.time.Duration;

public final class RetryPolicy {

    private final Duration initial;
    private final Duration max;

    public RetryPolicy(Duration initial, Duration max) {
        if (initial == null || max == null || initial.isZero() || initial.isNegative() || max.compareTo(initial) < 0) {
            throw new IllegalArgumentException("retry backoff must be > 0 and max-backoff >= initial-backoff");
        }
        this.initial = initial;
        this.max = max;
    }

    public long delayNanos(int attempt) {
        int shift = Math.min(Math.max(attempt, 1) - 1, 10);
        Duration delay = initial.multipliedBy(1L << shift);
        if (delay.compareTo(max) > 0 || delay.isNegative()) {
            delay = max;
        }
        return delay.toNanos();
    }
}
