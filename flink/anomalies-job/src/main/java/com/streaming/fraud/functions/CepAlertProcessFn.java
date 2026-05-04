package com.streaming.fraud.functions;

import com.streaming.fraud.model.Alert;
import com.streaming.fraud.model.Event;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class CepAlertProcessFn extends PatternProcessFunction<Event, Alert> {

    @Override
    public void processMatch(Map<String, List<Event>> match, Context ctx, Collector<Alert> out) throws Exception {

        Event approveEvent = match.get("approve").get(0);

        out.collect(new Alert(
                UUID.randomUUID().toString(),
                approveEvent.getCardId(),
                "DECLINED_PATTERN",
                Instant.now(),
                approveEvent.getTransactionId()
        ));

    }
}
