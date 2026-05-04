package com.streaming.fraud.deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.streaming.fraud.model.Event;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class EventDeserializer implements DeserializationSchema<Event> {

    private transient ObjectMapper objectMapper;

    @Override
    public void open(InitializationContext context) throws Exception {

        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());

    }

    @Override
    public Event deserialize(byte[] message) throws IOException {

        if (message == null || message.length == 0) return null;

        // this maps the msg bytes array to an Event object directly.
        return objectMapper.readValue(message, Event.class);

    }

    @Override
    public boolean isEndOfStream(Event event) {
        return false;
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return TypeInformation.of(Event.class); // let Flink know what data type is being deserialized
    }
}
