package ru.refiq.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableConfigurationProperties(DataIngestProperties.class)
public class IngestKafkaConfiguration {

    @Bean
    public ConsumerFactory<byte[], byte[]> ingestConsumerFactory(DataIngestProperties properties) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getKafka().getBootstrapServers());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, properties.getSource().getGroupId());
        config.put(ConsumerConfig.CLIENT_ID_CONFIG, "data-ingest-" + properties.getSource().getType());
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, properties.getBatch().getMaxRecords());
        config.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, (int) properties.getKafka().getMaxPollInterval().toMillis());
        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean
    public ProducerFactory<byte[], byte[]> dlqProducerFactory(DataIngestProperties properties) {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getKafka().getBootstrapServers());
        config.put(ProducerConfig.CLIENT_ID_CONFIG, "data-ingest-" + properties.getSource().getType() + "-dlq");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<byte[], byte[]> dlqKafkaTemplate(ProducerFactory<byte[], byte[]> dlqProducerFactory) {
        return new KafkaTemplate<>(dlqProducerFactory);
    }
}
