package com.streaming.fraud.functions;

import com.streaming.fraud.model.Alert;
import com.streaming.fraud.model.Event;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.UUID;

public class BurstAlertWindowFn extends ProcessWindowFunction<Event, Alert, String, TimeWindow> {

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
            out.collect(new Alert(
                    UUID.randomUUID().toString(),
                    cardId,
                    "BURST",
                    Instant.now(),
                    lastEvent != null
                            ? lastEvent.getTransactionId()
                            : "N/A"
            ));
        }

    }
}
