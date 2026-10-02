package ru.refiq.batch;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class IngestBatch {

    private final List<IngestRecord> records;
    private final FlushReason reason;
    private final long bytes;

    private IngestBatch(List<IngestRecord> records, FlushReason reason, long bytes) {
        this.records = List.copyOf(records);
        this.reason = reason;
        this.bytes = bytes;
    }

    public static IngestBatch of(List<IngestRecord> records, FlushReason reason) {
        long bytes = 0;
        for (IngestRecord record : records) {
            bytes += record.estimatedBytes();
        }
        return new IngestBatch(records, reason, bytes);
    }

    public List<IngestRecord> records() {
        return records;
    }

    public FlushReason reason() {
        return reason;
    }

    public long bytes() {
        return bytes;
    }

    public int size() {
        return records.size();
    }

    public boolean isEmpty() {
        return records.isEmpty();
    }

    public IngestBatch only(Set<TopicPartition> partitions) {
        return filter(partitions, true);
    }

    public IngestBatch except(Set<TopicPartition> partitions) {
        return filter(partitions, false);
    }

    private IngestBatch filter(Set<TopicPartition> partitions, boolean include) {
        List<IngestRecord> selected = new ArrayList<>();
        for (IngestRecord record : records) {
            boolean member = partitions.contains(record.topicPartition());
            if (member == include) {
                selected.add(record);
            }
        }
        return of(selected, reason);
    }

    public Map<TopicPartition, OffsetAndMetadata> commitOffsets() {
        Map<TopicPartition, Long> maxOffsets = new LinkedHashMap<>();
        for (IngestRecord record : records) {
            maxOffsets.merge(record.topicPartition(), record.offset(), Math::max);
        }
        Map<TopicPartition, OffsetAndMetadata> commits = new LinkedHashMap<>();
        maxOffsets.forEach((partition, offset) -> commits.put(partition, new OffsetAndMetadata(offset + 1)));
        return commits;
    }
}
