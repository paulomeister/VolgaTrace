package com.streaming.fraud.functions;

import com.streaming.fraud.model.Alert;
import com.streaming.fraud.model.Event;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.UUID;

public class BurstAlertWindowFn extends ProcessWindowFunction<Event, Alert, String, TimeWindow> {

    private static final Logger log = LoggerFactory.getLogger(BurstAlertWindowFn.class);
    private transient ValueState<Long> lastAlertWindowEndState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>(
                "last-burst-alert-window-end",
                Long.class
        );
        lastAlertWindowEndState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void process(String cardId,
                        ProcessWindowFunction<Event, Alert, String, TimeWindow>.Context context,
                        Iterable<Event> elements, Collector<Alert> out) throws Exception {

        long count = 0;
        Event lastEvent = null;

        for (Event e: elements) {
            count++;
            lastEvent = e;
        }

        if (count > 5) {
            long windowStart = context.window().getStart();
            long windowEnd = context.window().getEnd();
            Long lastAlertWindowEnd = lastAlertWindowEndState.value();

            if (lastAlertWindowEnd == null || windowStart >= lastAlertWindowEnd) {
                log.info("BURST detected: cardId={} count={} window=[{}, {}]", cardId, count,
                        windowStart, windowEnd);
                out.collect(new Alert(
                        UUID.randomUUID().toString(),
                        cardId,
                        "BURST",
                        Instant.now(),
                        lastEvent != null
                                ? lastEvent.getTransactionId()
                                : "N/A"
                ));
                lastAlertWindowEndState.update(windowEnd);
            } else {
                log.debug("BURST suppressed (overlap): cardId={} window=[{}, {}] lastAlertWindowEnd={}",
                        cardId, windowStart, windowEnd, lastAlertWindowEnd);
            }
        }

    }
}
