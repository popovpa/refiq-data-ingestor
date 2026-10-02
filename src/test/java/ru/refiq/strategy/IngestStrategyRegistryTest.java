package ru.refiq.strategy;

import org.testng.annotations.Test;
import ru.refiq.batch.IngestBatch;
import ru.refiq.error.FatalIngestException;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.sameInstance;
import static org.testng.Assert.expectThrows;

public class IngestStrategyRegistryTest {

    @Test
    public void findsStrategyByConfiguredType() {
        IngestStrategy clickstream = strategy("clickstream");

        IngestStrategyRegistry registry = new IngestStrategyRegistry(List.of(clickstream, strategy("audit")));

        assertThat(registry.require("clickstream"), sameInstance(clickstream));
    }

    @Test
    public void unknownTypeFailsStartup() {
        IngestStrategyRegistry registry = new IngestStrategyRegistry(List.of(strategy("clickstream")));

        expectThrows(FatalIngestException.class, () -> registry.require("conversion"));
    }

    @Test
    public void duplicateTypeFailsStartup() {
        expectThrows(
                FatalIngestException.class,
                () -> new IngestStrategyRegistry(List.of(strategy("clickstream"), strategy("clickstream")))
        );
    }

    private static IngestStrategy strategy(String type) {
        return new IngestStrategy() {
            @Override
            public String type() {
                return type;
            }

            @Override
            public void process(IngestBatch batch) {
            }
        };
    }
}
