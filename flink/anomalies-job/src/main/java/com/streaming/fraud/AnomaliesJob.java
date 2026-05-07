package com.streaming.fraud;

import com.streaming.fraud.deserializer.AlertSerializer;
import com.streaming.fraud.deserializer.IngestRecordDeserializer;
import com.streaming.fraud.functions.AmountAnomalyFn;
import com.streaming.fraud.functions.BurstAlertWindowFn;
import com.streaming.fraud.functions.CepAlertProcessFn;
import com.streaming.fraud.functions.DlqFormatFn;
import com.streaming.fraud.model.Alert;
import com.streaming.fraud.model.Event;
import com.streaming.fraud.model.IngestRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class AnomaliesJob {

    private static final String DEFAULT_BOOTSTRAP = "localhost:9092";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(); // flink environment.

        env.enableCheckpointing(60000); // fault tolerance for preserving state because of Welford algorithm.
        env.getConfig().setAutoWatermarkInterval(1000L);
        String bootstrapServers = resolveBootstrapServers(args);
        KafkaSource<IngestRecord> source = KafkaSource.<IngestRecord>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics("transactions-online", "transactions-pos")
                .setGroupId("fraud-detection-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(new IngestRecordDeserializer())
                .build();

        // uses event_timestamp field in watermark strategy
        WatermarkStrategy<Event> watermarkStrategy = WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withIdleness(Duration.ofSeconds(30))
                .withTimestampAssigner((event, timestamp) -> event.getEventTimestamp().toEpochMilli());

        Logger log = LoggerFactory.getLogger(AnomaliesJob.class);
        log.info("Starting job with DLQ and schema validation");
        log.info("Kafka bootstrap servers: {}", bootstrapServers);

        DataStream<IngestRecord> ingestStream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Kafka Ingest Source"
        );

        OutputTag<IngestRecord> invalidTag = new OutputTag<IngestRecord>("invalid-records") {};

        SingleOutputStreamOperator<Event> processedEvents = ingestStream
                .process(new ProcessFunction<IngestRecord, Event>() {
                    @Override
                    public void processElement(IngestRecord record, Context ctx, Collector<Event> out) throws Exception {
                        if (record == null) {
                            return;
                        }
                        if (!record.isValid()) {
                            ctx.output(invalidTag, record);
                            return;
                        }
                        Event event = record.getEvent();
                        if (event == null) {
                            log.warn("Invalid record with null event sent to side output");
                            ctx.output(invalidTag, record);
                            return;
                        }
                        out.collect(event);
                    }
                });

        DataStream<IngestRecord> invalidRecords = processedEvents.getSideOutput(invalidTag);

        DataStream<Event> validEvents = processedEvents
                .filter(event -> event.getEventTimestamp() != null && event.getCardId() != null);

        DataStream<String> dlqPayloads = invalidRecords.map(new DlqFormatFn());

        KafkaSink<String> dlqSink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("transactions-dlq")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        dlqPayloads.sinkTo(dlqSink);

        DataStream<Event> eventStream = validEvents.assignTimestampsAndWatermarks(watermarkStrategy);

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
        DataStream<Alert> cepAlerts = patternStream.process(new CepAlertProcessFn());

        // burst rule
        DataStream<Alert> freqAlerts = keyedStream
                .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(10)))
                        .process(new BurstAlertWindowFn());

        // alerts union
        DataStream<Alert> allAlerts = amountAlerts
                .union(cepAlerts)
                .union(freqAlerts);

        KafkaSink<Alert> sink = KafkaSink.<Alert>builder()
                        .setBootstrapServers(bootstrapServers)
                        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                                .setTopic("alerts")
                                .setValueSerializationSchema(new AlertSerializer())
                                .build())
                        .build();

        allAlerts.sinkTo(sink);
        allAlerts.print("Sinking to Kafka:alerts");

        env.execute("Fraud Detection, Anomalies Job"); // this throws Exception

    }

    private static String resolveBootstrapServers(String[] args) {
        String envValue = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        if (envValue != null && !envValue.trim().isEmpty()) {
            return envValue.trim();
        }
        if (args != null) {
            for (int i = 0; i < args.length; i++) {
                String arg = args[i];
                if (arg == null) {
                    continue;
                }
                if (arg.startsWith("--bootstrap.servers=")) {
                    return arg.substring("--bootstrap.servers=".length());
                }
                if ("--bootstrap.servers".equals(arg) && i + 1 < args.length) {
                    return args[i + 1];
                }
            }
        }
        return DEFAULT_BOOTSTRAP;
    }

}
