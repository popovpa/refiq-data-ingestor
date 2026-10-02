package ru.refiq.strategy.clickstream;

import java.util.List;

public record ClickstreamMapping(List<ClickstreamEventRecord> events, List<String> errors, boolean malformed) {

    public static ClickstreamMapping malformed(String error) {
        return new ClickstreamMapping(List.of(), List.of(error), true);
    }

    public static ClickstreamMapping of(List<ClickstreamEventRecord> events, List<String> errors) {
        return new ClickstreamMapping(List.copyOf(events), List.copyOf(errors), false);
    }
}
