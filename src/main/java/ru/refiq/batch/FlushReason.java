package ru.refiq.batch;

public enum FlushReason {
    SIZE("size"),
    BYTES("bytes"),
    TIMEOUT("timeout"),
    SHUTDOWN("shutdown"),
    REBALANCE("rebalance");

    private final String label;

    FlushReason(String label) {
        this.label = label;
    }

    public String label() {
        return label;
    }
}
