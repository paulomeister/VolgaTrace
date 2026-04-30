CREATE TABLE transactions (
    transaction_id STRING,
    card_id STRING,
    event_timestamp STRING,
    produced_at STRING,
    channel STRING,
    amount DOUBLE,
    currency STRING,
    status STRING,
    merchant_name STRING,
    merchant_category STRING,
    city STRING,
    country STRING,

    event_time AS CAST(
        REPLACE(SUBSTRING(event_timestamp, 1, 23), 'T', ' ')
        AS TIMESTAMP(3)
    ),

    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic-pattern' = 'transactions-(online|pos)',
    'properties.bootstrap.servers' = 'kafka:9094',
    'properties.group.id' = 'flink-aggregation-job',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

CREATE TABLE aggregation_results (
    window_start STRING,
    window_end STRING,
    channel STRING,
    currency STRING,
    merchant_category STRING,
    city STRING,
    transaction_count BIGINT,
    approved_count BIGINT,
    rejected_count BIGINT,
    pending_count BIGINT,
    avg_amount DOUBLE,
    max_amount DOUBLE
) WITH (
    'connector' = 'kafka',
    'topic' = 'transactions-aggregated',
    'properties.bootstrap.servers' = 'kafka:9094',
    'format' = 'json'
);

INSERT INTO aggregation_results
SELECT
    CAST(window_start AS STRING) AS window_start,
    CAST(window_end AS STRING) AS window_end,
    channel,
    currency,
    merchant_category,
    city,
    COUNT(*) AS transaction_count,
    SUM(CASE WHEN status = 'APPROVED' THEN 1 ELSE 0 END) AS approved_count,
    SUM(CASE WHEN status = 'REJECTED' THEN 1 ELSE 0 END) AS rejected_count,
    SUM(CASE WHEN status = 'PENDING' THEN 1 ELSE 0 END) AS pending_count,
    AVG(amount) AS avg_amount,
    MAX(amount) AS max_amount
FROM TABLE(
    TUMBLE(TABLE transactions, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
)
WHERE
    event_time IS NOT NULL
    AND amount IS NOT NULL
    AND channel IS NOT NULL
    AND merchant_category IS NOT NULL
GROUP BY
    window_start,
    window_end,
    channel,
    currency,
    merchant_category,
    city;