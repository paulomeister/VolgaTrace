package com.streaming.fraud;

import com.streaming.fraud.deserializer.EventDeserializer;
import com.streaming.fraud.functions.AmountAnomalyFn;
import com.streaming.fraud.model.Alert;
import com.streaming.fraud.model.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

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

        env.execute("Fraud Detection, Anomalies Job"); // this throws Exception

    }

}
