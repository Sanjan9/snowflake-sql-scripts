USE DATABASE MY_DATABASE;
USE SCHEMA MY_SCHEMA;
CREATE OR REPLACE TABLE MY_DATABASE.MY_SCHEMA.AAPL_STAGING (
    datetime TIMESTAMP,
    open FLOAT,
    close FLOAT,
    high FLOAT,
    low FLOAT,
    volume FLOAT
);

CREATE OR REPLACE TABLE MY_DATABASE.MY_SCHEMA.TSLA_STAGING (
    datetime TIMESTAMP,
    open FLOAT,
    close FLOAT,
    high FLOAT,
    low FLOAT,
    volume FLOAT
);

SHOW TABLES IN MY_DATABASE.MY_SCHEMA;

SELECT * FROM MY_DATABASE.MY_SCHEMA.AAPL_STAGING ORDER BY datetime DESC;
SELECT * FROM MY_DATABASE.MY_SCHEMA.TSLA_STAGING ORDER BY datetime DESC;


--View the Latest Data

SELECT * FROM MY_DATABASE.MY_SCHEMA.AAPL_STAGING
ORDER BY datetime DESC
LIMIT 10;


SELECT * FROM MY_DATABASE.MY_SCHEMA.TSLA_STAGING
ORDER BY datetime DESC
LIMIT 10;

--Find the Highest and Lowest Prices aapl
SELECT 
    MAX(high) AS highest_price,
    MIN(low) AS lowest_price
FROM MY_DATABASE.MY_SCHEMA.AAPL_STAGING;

--Find the Highest and Lowest Prices tsla
SELECT 
    MAX(high) AS highest_price,
    MIN(low) AS lowest_price
FROM MY_DATABASE.MY_SCHEMA.TSLA_STAGING;


--Calculate the Average Closing Price

SELECT 
    AVG(close) AS average_closing_price
FROM MY_DATABASE.MY_SCHEMA.AAPL_STAGING;

SELECT 
    AVG(close) AS average_closing_price
FROM MY_DATABASE.MY_SCHEMA.TSLA_STAGING;


--Find the Total Trading Volume Per Day

SELECT 
    DATE(datetime) AS trade_date,
    SUM(volume) AS total_volume
FROM MY_DATABASE.MY_SCHEMA.AAPL_STAGING
GROUP BY trade_date
ORDER BY trade_date DESC;

SELECT 
    DATE(datetime) AS trade_date,
    SUM(volume) AS total_volume
FROM MY_DATABASE.MY_SCHEMA.TSLA_STAGING
GROUP BY trade_date
ORDER BY trade_date DESC;


--Compare AAPL vs TSLA Performance

SELECT 
    'AAPL' AS stock, 
    AVG(close) AS avg_close, 
    MAX(high) AS max_high, 
    MIN(low) AS min_low
FROM MY_DATABASE.MY_SCHEMA.AAPL_STAGING

UNION ALL

SELECT 
    'TSLA' AS stock, 
    AVG(close) AS avg_close, 
    MAX(high) AS max_high, 
    MIN(low) AS min_low
FROM MY_DATABASE.MY_SCHEMA.TSLA_STAGING;

--Find Days When Prices Increased

SELECT datetime, open, close, 
       (close - open) AS price_change
FROM MY_DATABASE.MY_SCHEMA.AAPL_STAGING
WHERE close > open
ORDER BY datetime DESC;


SELECT datetime, open, close, 
       (close - open) AS price_change
FROM MY_DATABASE.MY_SCHEMA.TSLA_STAGING
WHERE close > open
ORDER BY datetime DESC;



--Find The Most Volatile Days (Largest Price Differences)

SELECT datetime, 
       (high - low) AS daily_range
FROM MY_DATABASE.MY_SCHEMA.AAPL_STAGING
ORDER BY daily_range DESC
LIMIT 5;


SELECT datetime, 
       (high - low) AS daily_range
FROM MY_DATABASE.MY_SCHEMA.TSLA_STAGING
ORDER BY daily_range DESC
LIMIT 5;


SELECT MAX(datetime) FROM MY_DATABASE.MY_SCHEMA.AAPL_STAGING;

SELECT * FROM MY_DATABASE.MY_SCHEMA.AAPL_STAGING 
WHERE DATE(datetime) = '2025-07-03';
