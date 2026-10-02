package ru.refiq.strategy;

import org.springframework.stereotype.Component;
import ru.refiq.error.FatalIngestException;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Component
public class IngestStrategyRegistry {

    private final Map<String, IngestStrategy> strategies;

    public IngestStrategyRegistry(List<IngestStrategy> strategies) {
        Map<String, IngestStrategy> collected = new LinkedHashMap<>();
        for (IngestStrategy strategy : strategies) {
            String type = strategy.type();
            if (type == null || type.isBlank()) {
                throw new FatalIngestException("Ingest strategy type is blank: " + strategy.getClass().getName());
            }
            IngestStrategy previous = collected.putIfAbsent(type, strategy);
            if (previous != null) {
                throw new FatalIngestException(
                        "Duplicate ingest strategy type '" + type + "' from "
                                + previous.getClass().getName() + " and " + strategy.getClass().getName()
                );
            }
        }
        this.strategies = Map.copyOf(collected);
    }

    public IngestStrategy require(String type) {
        IngestStrategy strategy = strategies.get(type);
        if (strategy == null) {
            throw new FatalIngestException("No ingest strategy for type '" + type + "'");
        }
        return strategy;
    }
}
