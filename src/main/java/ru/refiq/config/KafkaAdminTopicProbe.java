package ru.refiq.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.springframework.stereotype.Component;
import ru.refiq.error.FatalIngestException;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
public class KafkaAdminTopicProbe implements KafkaTopicProbe {

    private static final long TIMEOUT_MS = 10_000;

    private final DataIngestProperties properties;

    public KafkaAdminTopicProbe(DataIngestProperties properties) {
        this.properties = properties;
    }

    @Override
    public boolean topicExists(String topic) {
        Map<String, Object> config = Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getKafka().getBootstrapServers(),
                AdminClientConfig.CLIENT_ID_CONFIG, "data-ingest-metadata",
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) TIMEOUT_MS,
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) TIMEOUT_MS
        );
        try (AdminClient admin = AdminClient.create(config)) {
            Set<String> names = admin.listTopics(new ListTopicsOptions().timeoutMs((int) TIMEOUT_MS))
                    .names()
                    .get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
            return names.contains(topic);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FatalIngestException("Kafka metadata request interrupted", e);
        } catch (ExecutionException | TimeoutException e) {
            throw new FatalIngestException("Kafka metadata request failed", e);
        }
    }
}
