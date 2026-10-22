package ru.refiq.config;

import org.springframework.stereotype.Component;
import ru.refiq.storage.ClickHouseClient;

@Component
@ConditionalOnClickstreamPipeline
public class ClickHouseStorageStartupValidator implements IngestStorageStartupValidator {

    private final ClickHouseClient clickHouseClient;
    private final ClickHouseStartupValidator connectionValidator = new ClickHouseStartupValidator();

    public ClickHouseStorageStartupValidator(ClickHouseClient clickHouseClient) {
        this.clickHouseClient = clickHouseClient;
    }

    @Override
    public boolean supports(String type) {
        return "clickstream".equals(type);
    }

    @Override
    public void validate(DataIngestProperties properties) {
        connectionValidator.validate(properties.getStorage().getClickhouse(), clickHouseClient);
    }
}
