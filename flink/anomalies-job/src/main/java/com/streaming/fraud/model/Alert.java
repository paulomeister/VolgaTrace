package com.streaming.fraud.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Alert {

    @JsonProperty("alert_id")
    private String alertId;
    @JsonProperty("card_id")
    private String cardId;
    @JsonProperty("alert_type")
    private String alertType;
    @JsonProperty("triggered_at")
    private Instant triggeredAt;
    @JsonProperty("source_transaction_id")
    private String sourceTransactionId;

}
