package ru.refiq.config;

import org.testng.annotations.Test;
import ru.refiq.error.FatalIngestException;
import ru.refiq.support.TestIngest;

import java.time.Duration;

import static org.testng.Assert.expectThrows;

public class IngestStartupCheckerTest {

    @Test
    public void acceptsACompleteConfiguration() {
        IngestStartupChecker.validate(TestIngest.properties());
    }

    @Test
    public void rejectsPollTimeoutThatCannotFlushOnTime() {
        DataIngestProperties properties = TestIngest.properties();
        properties.getBatch().setPollTimeout(Duration.ofSeconds(2));

        expectThrows(FatalIngestException.class, () -> IngestStartupChecker.validate(properties));
    }

    @Test
    public void rejectsMissingTopic() {
        DataIngestProperties properties = TestIngest.properties();
        properties.getSource().setTopic(" ");

        expectThrows(FatalIngestException.class, () -> IngestStartupChecker.validate(properties));
    }

    @Test
    public void rejectsInvalidStorage() {
        DataIngestProperties properties = TestIngest.properties();
        properties.getStorage().getClickhouse().setTable("clickstream-events");

        expectThrows(FatalIngestException.class, () -> IngestStartupChecker.validate(properties));
    }
}
