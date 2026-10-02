package ru.refiq.error;

import ru.refiq.batch.IngestRecord;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;

public record DeadLetter(
        String sourceTopic,
        int partition,
        long offset,
        String key,
        String keyBase64,
        String payload,
        String payloadBase64,
        ErrorCategory errorCategory,
        String errorDescription,
        String ingestType,
        Instant timestamp
) {

    public static DeadLetter invalid(IngestRecord record, String ingestType, String description) {
        return of(record, ingestType, ErrorCategory.INVALID, description);
    }

    public static DeadLetter of(IngestRecord record, String ingestType, ErrorCategory category, String description) {
        return new DeadLetter(
                record.topic(),
                record.partition(),
                record.offset(),
                utf8OrNull(record.key()),
                record.key() == null ? null : Base64.getEncoder().encodeToString(record.key()),
                utf8OrEmpty(record.value()),
                record.value() == null ? "" : Base64.getEncoder().encodeToString(record.value()),
                category,
                description,
                ingestType,
                Instant.now()
        );
    }

    private static String utf8OrNull(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        return decodeUtf8(bytes);
    }

    private static String utf8OrEmpty(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return "";
        }
        String decoded = decodeUtf8(bytes);
        return decoded == null ? "" : decoded;
    }

    private static String decodeUtf8(byte[] bytes) {
        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        try {
            return decoder.decode(java.nio.ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException e) {
            return null;
        }
    }
}
