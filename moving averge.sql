--we need to store historical trends and moving averages, let’s create a new table in Snowflake.
CREATE OR REPLACE TABLE MY_DATABASE.MY_SCHEMA.AAPL_ANALYSIS (
    datetime TIMESTAMP,
    close FLOAT,
    moving_ avg_7 FLOAT
);

CREATE OR REPLACE TABLE MY_DATABASE.MY_SCHEMA.TSLA_ANALYSIS (
    datetime TIMESTAMP,
    close FLOAT,
    moving_avg_7 FLOAT
);

--Calculate the Moving Average in SQL
--let’s compute the 7-day moving average for closing prices
INSERT INTO MY_DATABASE.MY_DATABASEMY_DATABASE.MY_SCHEMA.AAPL_ANALYSIS.AAPL_ANALYSIS (datetime, close, moving_avg_7)
SELECT datetime, close,
       AVG(close) OVER (ORDER BY datetime ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS moving_avg_7
FROM MY_DATABASE.MY_SCHEMA.AAPL_STAGING;


INSERT INTO MY_DATABASE.MY_SCHEMA.TSLA_ANALYSIS (datetime, close, moving_avg_7)
SELECT datetime, close,
       AVG(close) OVER (ORDER BY datetime ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS moving_avg_7
FROM MY_DATABASE.MY_SCHEMA.TSLA_STAGING;

---Verify the Moving Averages
--After inserting data, lets check if the moving averages were stored correctly.

SELECT * FROM MY_DATABASE.MY_SCHEMA.AAPL_ANALYSIS
ORDER BY datetime DESC
LIMIT 10;


SELECT * FROM MY_DATABASE.MY_SCHEMA.TSLA_ANALYSIS
ORDER BY datetime 


