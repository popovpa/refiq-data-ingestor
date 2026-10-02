package ru.refiq.consumer;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

public interface OffsetCommitter {

    void commit(Map<TopicPartition, OffsetAndMetadata> offsets);
}
