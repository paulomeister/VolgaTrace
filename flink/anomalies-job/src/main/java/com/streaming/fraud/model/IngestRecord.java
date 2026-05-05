package com.streaming.fraud.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class IngestRecord {

    private Event event;
    private String raw;
    private String error;
    private boolean valid;

    private String topic;
    private Integer partition;
    private Long offset;
    private String key;

}

