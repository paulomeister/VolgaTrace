package com.streaming.fraud.functions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.streaming.fraud.model.DlqRecord;
import com.streaming.fraud.model.IngestRecord;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

public class DlqFormatFn extends RichMapFunction<IngestRecord, String> {

    private static final Logger log = LoggerFactory.getLogger(DlqFormatFn.class);
    private transient ObjectMapper objectMapper;

    @Override
    public void open(Configuration parameters) {
        objectMapper = new ObjectMapper();
    }

    @Override
    public String map(IngestRecord record) throws Exception {
        DlqRecord dlq = new DlqRecord(
                record.getRaw(),
                record.getError(),
                record.getTopic(),
                record.getPartition(),
                record.getOffset(),
                record.getKey(),
                Instant.now().toString()
        );
        String payload = objectMapper.writeValueAsString(dlq);
        log.warn("DLQ: topic={} partition={} offset={} error={}",
                record.getTopic(), record.getPartition(), record.getOffset(), record.getError());
        return payload;
    }
}

