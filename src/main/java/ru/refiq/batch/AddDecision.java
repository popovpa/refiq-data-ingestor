package ru.refiq.batch;

public final class AddDecision {

    public enum Type {
        ACCEPTED,
        ACCEPTED_FLUSH,
        FLUSH_BEFORE_ADD,
        OVERSIZED
    }

    private final Type type;
    private final FlushReason reason;

    private AddDecision(Type type, FlushReason reason) {
        this.type = type;
        this.reason = reason;
    }

    public static AddDecision accepted() {
        return new AddDecision(Type.ACCEPTED, null);
    }

    public static AddDecision acceptedFlush(FlushReason reason) {
        return new AddDecision(Type.ACCEPTED_FLUSH, reason);
    }

    public static AddDecision flushBeforeAdd(FlushReason reason) {
        return new AddDecision(Type.FLUSH_BEFORE_ADD, reason);
    }

    public static AddDecision oversized() {
        return new AddDecision(Type.OVERSIZED, null);
    }

    public Type type() {
        return type;
    }

    public FlushReason reason() {
        return reason;
    }
}
