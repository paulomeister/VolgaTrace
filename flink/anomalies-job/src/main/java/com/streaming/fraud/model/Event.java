package com.streaming.fraud.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Event {

    @JsonProperty("transaction_id")
    private String transactionId;
    @JsonProperty("card_id")
    private String cardId;
    @JsonProperty("event_timestamp")
    private Instant eventTimestamp;
    @JsonProperty("produced_at")
    private Instant producedAt;
    @JsonProperty("channel")
    private String channel;
    @JsonProperty("amount")
    private Double amount;
    @JsonProperty("currency")
    private String currency;
    @JsonProperty("status")
    private String status;
    @JsonProperty("merchant_name")
    private String merchantName;
    @JsonProperty("merchant_category")
    private String merchantCategory;
    @JsonProperty("city")
    private String city;
    @JsonProperty("country")
    private String country;


}
