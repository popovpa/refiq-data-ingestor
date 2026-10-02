package ru.refiq.config;

public interface KafkaTopicProbe {

    boolean topicExists(String topic);
}
