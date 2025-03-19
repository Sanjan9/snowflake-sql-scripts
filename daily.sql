--Latest Data Check

SELECT * FROM MY_DATABASE.MY_SCHEMA.STOCK_HISTORY
WHERE end_date = '2099-12-31 23:59:59'  -- 🔹 Instead of "is_current = TRUE"
ORDER BY datetime DESC
LIMIT 10;


---Historical Data Check

SELECT * FROM MY_DATABASE.MY_SCHEMA.STOCK_HISTORY
WHERE end_date != '2099-12-31 23:59:59'  -- 🔹 Instead of "IS NOT NULL"
ORDER BY end_date DESC
LIMIT 10;


--- to check if todays data is inserted or not

SELECT stock_symbol, MAX(datetime) AS last_data_time
FROM MY_DATABASE.MY_SCHEMA.STOCK_HISTORY
WHERE DATE(datetime) = CURRENT_DATE  -- 🔹 Ensures today’s data exists
GROUP BY stock_symbol;


--Count Check
SELECT stock_symbol, COUNT(*) AS total_records
FROM MY_DATABASE.MY_SCHEMA.STOCK_HISTORY
GROUP BY stock_symbol;



---Check Airflow DAG Execution

SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
WHERE NAME = 'fetch_stock_data_to_snowflake'
ORDER BY SCHEDULED_TIME DESC
LIMIT 5;


----

SHOW TASKS



----fetching some sample records for testing 

-- Fetch sample records from staging
SELECT * FROM MY_DATABASE.MY_SCHEMA.AAPL_STAGING ORDER BY datetime DESC LIMIT 5;

-- Fetch sample records from archive
SELECT * FROM MY_DATABASE.MY_SCHEMA.AAPL_ARCHIVE ORDER BY datetime DESC LIMIT 5;


-- Insert an expired record (Yesterday's data)
INSERT INTO MY_DATABASE.MY_SCHEMA.STOCK_HISTORY 
(stock_symbol, datetime, open, close, high, low, volume, start_date, end_date, is_current)
VALUES 
('AAPL', '2024-03-02 10:00:00', 175.0, 178.0, 180.0, 174.0, 50000, '2024-03-02 10:00:00', '2024-03-03 10:00:00', FALSE);

-- Insert an active record (Today's data, expiry date = 2099-12-31)
INSERT INTO MY_DATABASE.MY_SCHEMA.STOCK_HISTORY 
(stock_symbol, datetime, open, close, high, low, volume, start_date, end_date, is_current)
VALUES 
('AAPL', '2024-03-03 10:00:00', 180.0, 182.0, 185.0, 178.0, 52000, '2024-03-03 10:00:00', '2099-12-31 23:59:59', TRUE);


-- Check all records, sorted by datetime
SELECT * FROM MY_DATABASE.MY_SCHEMA.STOCK_HISTORY ORDER BY datetime DESC;

-- Check active records (Should have end_date = 2099-12-31)
SELECT * FROM MY_DATABASE.MY_SCHEMA.STOCK_HISTORY WHERE end_date = '2099-12-31 23:59:59';

-- Check expired records (Should have yesterday’s date)
SELECT * FROM MY_DATABASE.MY_SCHEMA.STOCK_HISTORY WHERE end_date != '2099-12-31 23:59:59';







