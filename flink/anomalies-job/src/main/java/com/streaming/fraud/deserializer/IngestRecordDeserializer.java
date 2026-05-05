package com.streaming.fraud.deserializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.streaming.fraud.model.Event;
import com.streaming.fraud.model.IngestRecord;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

public class IngestRecordDeserializer implements KafkaRecordDeserializationSchema<IngestRecord> {

    private static final Logger log = LoggerFactory.getLogger(IngestRecordDeserializer.class);
    private transient ObjectMapper objectMapper;

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<IngestRecord> out) {
        byte[] value = record.value();
        String raw = value == null ? null : new String(value, StandardCharsets.UTF_8);

        if (raw == null || raw.isBlank()) {
            out.collect(invalid("empty-message", raw, record));
            return;
        }

        try {
            JsonNode root = objectMapper.readTree(raw);
            validateSchema(root);
            Event event = objectMapper.treeToValue(root, Event.class);
            out.collect(valid(event, raw, record));
        } catch (Exception ex) {
            out.collect(invalid(ex.getMessage(), raw, record));
            log.warn("Evento invalido enviado a DLQ: topic={} partition={} offset={} error={}",
                    record.topic(), record.partition(), record.offset(), ex.getMessage());
        }
    }

    private void validateSchema(JsonNode root) {
        requireText(root, "transaction_id");
        requireText(root, "card_id");
        requireText(root, "event_timestamp");
        requireText(root, "produced_at");
        requireText(root, "channel");
        requireNumber(root, "amount");
        requireText(root, "currency");
        requireText(root, "status");
        requireText(root, "merchant_name");
        requireText(root, "merchant_category");
        requireText(root, "city");
        requireText(root, "country");

        Instant.parse(root.get("event_timestamp").asText());
        Instant.parse(root.get("produced_at").asText());
    }

    private void requireText(JsonNode root, String field) {
        JsonNode node = root.get(field);
        if (node == null || node.isNull() || !node.isTextual() || node.asText().isBlank()) {
            throw new IllegalArgumentException("missing-or-invalid-field: " + field);
        }
    }

    private void requireNumber(JsonNode root, String field) {
        JsonNode node = root.get(field);
        if (node == null || node.isNull() || !node.isNumber()) {
            throw new IllegalArgumentException("missing-or-invalid-field: " + field);
        }
    }

    private IngestRecord valid(Event event, String raw, ConsumerRecord<byte[], byte[]> record) {
        return new IngestRecord(
                event,
                raw,
                null,
                true,
                record.topic(),
                record.partition(),
                record.offset(),
                record.key() == null ? null : new String(record.key(), StandardCharsets.UTF_8)
        );
    }

    private IngestRecord invalid(String error, String raw, ConsumerRecord<byte[], byte[]> record) {
        return new IngestRecord(
                null,
                raw,
                error,
                false,
                record.topic(),
                record.partition(),
                record.offset(),
                record.key() == null ? null : new String(record.key(), StandardCharsets.UTF_8)
        );
    }

    @Override
    public TypeInformation<IngestRecord> getProducedType() {
        return TypeInformation.of(IngestRecord.class);
    }
}

