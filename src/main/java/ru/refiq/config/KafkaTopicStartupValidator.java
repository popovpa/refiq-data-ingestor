package ru.refiq.config;

import ru.refiq.error.FatalIngestException;

public class KafkaTopicStartupValidator {

    public void validate(String topic, KafkaTopicProbe probe) {
        boolean exists;
        try {
            exists = probe.topicExists(topic);
        } catch (FatalIngestException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new FatalIngestException("Kafka metadata request failed", e);
        }
        if (!exists) {
            throw new FatalIngestException("Kafka topic does not exist: " + topic);
        }
    }
}
