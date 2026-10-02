package ru.refiq.batch;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;

import java.util.ArrayList;
import java.util.List;

public final class IngestRecord {

    private final String topic;
    private final int partition;
    private final long offset;
    private final byte[] key;
    private final byte[] value;
    private final long estimatedBytes;
    private final long enqueuedAtNanos;

    public IngestRecord(
            String topic,
            int partition,
            long offset,
            byte[] key,
            byte[] value,
            long estimatedBytes,
            long enqueuedAtNanos
    ) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.key = key;
        this.value = value;
        this.estimatedBytes = estimatedBytes;
        this.enqueuedAtNanos = enqueuedAtNanos;
    }

    public static List<IngestRecord> from(ConsumerRecords<byte[], byte[]> records, long enqueuedAtNanos) {
        List<IngestRecord> converted = new ArrayList<>();
        for (ConsumerRecord<byte[], byte[]> record : records) {
            converted.add(from(record, enqueuedAtNanos));
        }
        return converted;
    }

    public static IngestRecord from(ConsumerRecord<byte[], byte[]> record, long enqueuedAtNanos) {
        long bytes = size(record.key()) + size(record.value());
        for (Header header : record.headers()) {
            bytes += header.key() == null ? 0 : header.key().length();
            bytes += size(header.value());
        }
        return new IngestRecord(
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                record.value(),
                bytes,
                enqueuedAtNanos
        );
    }

    private static long size(byte[] bytes) {
        return bytes == null ? 0 : bytes.length;
    }

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }

    public long offset() {
        return offset;
    }

    public byte[] key() {
        return key;
    }

    public byte[] value() {
        return value;
    }

    public long estimatedBytes() {
        return estimatedBytes;
    }

    public long enqueuedAtNanos() {
        return enqueuedAtNanos;
    }

    public TopicPartition topicPartition() {
        return new TopicPartition(topic, partition);
    }
}
