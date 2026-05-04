package com.streaming.fraud;

import com.streaming.fraud.deserializer.EventDeserializer;
import com.streaming.fraud.functions.AmountAnomalyFn;
import com.streaming.fraud.model.Alert;
import com.streaming.fraud.model.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class AnomaliesJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(); // flink environment.

        env.enableCheckpointing(60000); // fault tolerance for preserving state because of Welford algorithm.

        KafkaSource<Event> source = KafkaSource.<Event>builder()
                .setBootstrapServers("localhost:9094")
                .setTopics("transactions-online", "transactions-pos")
                .setGroupId("fraud-detection-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new EventDeserializer()))
                .build();

        // uses event_timestamp field in watermark strategy
        WatermarkStrategy<Event> watermarkStrategy = WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, timestamp) -> event.getEventTimestamp().toEpochMilli());

        DataStream<Event> eventStream = env.fromSource(source, watermarkStrategy, "Kafka Events Source");

        KeyedStream<Event, String> keyedStream = eventStream.keyBy(Event::getCardId);

        DataStream<Alert> amountAlerts = keyedStream.process(new AmountAnomalyFn());

        // CEP rule for rejected*3->approved pattern
        Pattern<Event, ?> declinedPattern = Pattern.<Event>begin("rejects")
                        .where(new SimpleCondition<Event>() {
                            @Override
                            public boolean filter(Event value) throws Exception {
                                return "REJECTED".equalsIgnoreCase(value.getStatus());
                            }
                        })
                                .times(3).consecutive()
                        .next("approve")
                                .where(new SimpleCondition<Event>() {
                                    @Override
                                    public boolean filter(Event value) throws Exception {
                                        return "APPROVED".equalsIgnoreCase(value.getStatus());
                                    }
                                })
                                        .within(Time.minutes(10));

        // subscribe pattern to keyed stream
        PatternStream<Event> patternStream = CEP.pattern(keyedStream, declinedPattern);

        // search for coincidences and trigger alerts
        DataStream<Alert> cepAlerts = patternStream.process(
                new PatternProcessFunction<Event, Alert>() {
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
        );

        env.execute("Fraud Detection, Anomalies Job"); // this throws Exception

    }

}
