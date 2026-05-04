package com.streaming.fraud.functions;

import com.streaming.fraud.model.Alert;
import com.streaming.fraud.model.Event;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.UUID;

public class AmountAnomalyFn extends KeyedProcessFunction<String, Event, Alert> {

    private transient ValueState<Long> countState;
    private transient ValueState<Double> meanState;
    private transient ValueState<Double> m2State;

    @Override
    public void open(Configuration parameters) throws Exception {

        // ttl to prune inactive cards from memory
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.days(7))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .build();

        ValueStateDescriptor<Long> countDesc = new ValueStateDescriptor<>("count", Long.class);
        countDesc.enableTimeToLive(ttlConfig);
        countState = getRuntimeContext().getState(countDesc);

        ValueStateDescriptor<Double> meanDesc = new ValueStateDescriptor<>("mean", Double.class);
        meanDesc.enableTimeToLive(ttlConfig);
        meanState = getRuntimeContext().getState(meanDesc);

        ValueStateDescriptor<Double> m2Desc = new ValueStateDescriptor<>("m2", Double.class);
        m2Desc.enableTimeToLive(ttlConfig);
        m2State = getRuntimeContext().getState(m2Desc);

    }

    @Override
    public void processElement(Event event, KeyedProcessFunction<String, Event, Alert>.Context ctx, Collector<Alert> out) throws Exception {

        Double amount = event.getAmount();
        String currency = event.getCurrency();

        if (amount == null) return;

        // code for static amount threshold business rule enforcement
        boolean isStaticAnomaly = false;
        String staticReason = "";

        if ("USD".equalsIgnoreCase(currency) && amount > 5_000) {
            isStaticAnomaly = true;
            staticReason = "Transaction amount exceeds static amount threshold of 5000 USD";
        }
        else if ("COP".equalsIgnoreCase(currency) && amount > 15_000_000) {
            isStaticAnomaly = true;
            staticReason = "Transaction amount exceeds static amount threshold of 5000 USD";
        }

        if (isStaticAnomaly) {
            out.collect(new Alert(
                    UUID.randomUUID().toString(),
                    event.getCardId(),
                    "HIGH AMOUNT",
                    Instant.now(),
                    event.getTransactionId()
            ));

            return;
        }

        // Welford algorithm
        Long currentCount = countState.value();
        Double currentMean = meanState.value();
        Double currentM2 = m2State.value();

        long n = (currentCount == 0) ? 0L : currentCount;
        double mu = (currentMean == 0) ? 0L : currentMean;
        double m2 = (currentM2 == 0) ? 0L : currentM2;

        // update
        n++;
        double delta = amount - mu;
        mu += delta / n;
        double delta2 = amount - mu;
        m2 = delta * delta2;

        countState.update(n);
        meanState.update(mu);
        m2State.update(m2);

        // check Z-score upon 5 previous data for a given card
        if (n > 5) {
            double variance = m2 / (n - 1);
            double std = Math.sqrt(variance);

            // how many std is the amount away from the mean
            double zScore = (std > 0) ? Math.abs((amount - mu) / std) : 0.0;

            if (zScore >= 3.0) {
                out.collect(new Alert(
                        UUID.randomUUID().toString(),
                        event.getCardId(),
                        "HIGH AMOUNT",
                        Instant.now(),
                        event.getTransactionId()
                ));
            }
        }

    }
}
