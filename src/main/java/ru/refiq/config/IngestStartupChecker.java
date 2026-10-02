package ru.refiq.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;
import ru.refiq.error.FatalIngestException;
import ru.refiq.storage.ClickHouseClient;
import ru.refiq.strategy.IngestStrategy;
import ru.refiq.strategy.IngestStrategyRegistry;

import java.net.URI;
import java.time.Duration;
import java.util.regex.Pattern;

@Component
public class IngestStartupChecker implements InitializingBean {

    private static final Logger log = LoggerFactory.getLogger(IngestStartupChecker.class);
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]{0,63}");

    private final DataIngestProperties properties;
    private final IngestStrategyRegistry registry;
    private final KafkaTopicProbe kafkaTopicProbe;
    private final ClickHouseClient clickHouseClient;
    private final InfrastructureReadiness readiness;
    private final KafkaTopicStartupValidator kafkaTopicStartupValidator = new KafkaTopicStartupValidator();
    private final ClickHouseStartupValidator clickHouseStartupValidator = new ClickHouseStartupValidator();

    public IngestStartupChecker(
            DataIngestProperties properties,
            IngestStrategyRegistry registry,
            KafkaTopicProbe kafkaTopicProbe,
            ClickHouseClient clickHouseClient,
            InfrastructureReadiness readiness
    ) {
        this.properties = properties;
        this.registry = registry;
        this.kafkaTopicProbe = kafkaTopicProbe;
        this.clickHouseClient = clickHouseClient;
        this.readiness = readiness;
    }

    @Override
    public void afterPropertiesSet() {
        validate(properties);
        IngestStrategy strategy = registry.require(properties.getSource().getType());
        kafkaTopicStartupValidator.validate(properties.getSource().getTopic(), kafkaTopicProbe);
        clickHouseStartupValidator.validate(properties.getStorage().getClickhouse(), clickHouseClient);
        readiness.markReady();
        log.info(
                "data-ingest ready topic={} type={} strategy={} groupId={} monitoringPort={}",
                properties.getSource().getTopic(),
                properties.getSource().getType(),
                strategy.getClass().getName(),
                properties.getSource().getGroupId(),
                properties.getMonitoring().getPort()
        );
    }

    public static void validate(DataIngestProperties properties) {
        DataIngestProperties.Source source = properties.getSource();
        if (source == null || isBlank(source.getTopic()) || isBlank(source.getType()) || isBlank(source.getGroupId())) {
            throw new FatalIngestException("topic, type and group-id must be set");
        }
        DataIngestProperties.Batch batch = properties.getBatch();
        if (batch == null
                || batch.getMaxRecords() <= 0
                || batch.getMaxBytes() <= 0
                || !positive(batch.getMaxWait())
                || !positive(batch.getPollTimeout())
                || batch.getPollTimeout().compareTo(batch.getMaxWait()) > 0) {
            throw new FatalIngestException("batch limits must be > 0 and poll-timeout must be <= max-wait");
        }
        DataIngestProperties.Kafka kafka = properties.getKafka();
        DataIngestProperties.ClickHouse clickHouse = properties.getStorage() == null ? null : properties.getStorage().getClickhouse();
        if (kafka == null
                || isBlank(kafka.getBootstrapServers())
                || !positive(kafka.getMaxPollInterval())
                || clickHouse == null
                || !positive(clickHouse.getRequestTimeout())
                || kafka.getMaxPollInterval().compareTo(clickHouse.getRequestTimeout()) <= 0) {
            throw new FatalIngestException("kafka and clickhouse timeouts must allow a storage request to finish inside max.poll.interval");
        }
        DataIngestProperties.Retry retry = properties.getRetry();
        if (retry == null
                || !positive(retry.getInitialBackoff())
                || !positive(retry.getMaxBackoff())
                || retry.getMaxBackoff().compareTo(retry.getInitialBackoff()) < 0) {
            throw new FatalIngestException("retry backoff must be > 0 and max-backoff >= initial-backoff");
        }
        if (properties.getMonitoring() == null
                || properties.getMonitoring().getPort() < 1
                || properties.getMonitoring().getPort() > 65535) {
            throw new FatalIngestException("monitoring port must be between 1 and 65535");
        }
        if (clickHouse.getUrl() == null || clickHouse.getUsername() == null || clickHouse.getPassword() == null) {
            throw new FatalIngestException("clickhouse storage configuration is incomplete");
        }
        URI uri;
        try {
            uri = URI.create(clickHouse.getUrl());
        } catch (IllegalArgumentException e) {
            throw new FatalIngestException("clickhouse url is invalid");
        }
        if (uri.getScheme() == null || !(uri.getScheme().equals("http") || uri.getScheme().equals("https")) || uri.getHost() == null) {
            throw new FatalIngestException("clickhouse url must be an http(s) endpoint");
        }
        if (isBlank(clickHouse.getDatabase())
                || isBlank(clickHouse.getTable())
                || !IDENTIFIER.matcher(clickHouse.getDatabase()).matches()
                || !IDENTIFIER.matcher(clickHouse.getTable()).matches()) {
            throw new FatalIngestException("clickhouse database and table must be identifiers");
        }
        if (isBlank(clickHouse.getUsername())) {
            throw new FatalIngestException("clickhouse username must be set");
        }
    }

    private static boolean positive(Duration duration) {
        return duration != null && duration.isPositive();
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
