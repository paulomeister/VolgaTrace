package com.streaming.fraud.deserializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.streaming.fraud.model.Alert;
import org.apache.flink.api.common.serialization.SerializationSchema;

public class AlertSerializer implements SerializationSchema<Alert> {

    private transient ObjectMapper objectMapper;

    @Override
    public void open(InitializationContext context) throws Exception {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Override
    public byte[] serialize(Alert alert) {
        try {
            return objectMapper.writeValueAsBytes(alert);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException("Error while serializing alert", e);
        }
    }
}
