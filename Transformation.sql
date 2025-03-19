-- Creation of Final Table

-- USE WAREHOUSE COMPUTE_WH;

-- CREATE OR REPLACE TABLE MY_DATABASE.MY_SCHEMA.FINAL_STOCK_TABLE (
--     symbol STRING,          
--     start_date DATE,        
--     end_date DATE,          
--     open FLOAT,            
--     close FLOAT,      
--     high FLOAT,         
--     low FLOAT,             
--     volume FLOAT,         
--     indicator INT DEFAULT 1 
-- );

-- Insert Dummy Data for Test
-- INSERT INTO MY_DATABASE.MY_SCHEMA.FINAL_STOCK_TABLE 
-- (symbol, start_date, end_date, open, close, high, low, volume, indicator)
-- VALUES 
-- ('AAPL', '2025-03-02', '2099-12-31', 250.0, 255.0, 260.0, 245.0, 500000, 1);

-- INSERT INTO MY_DATABASE.MY_SCHEMA.FINAL_STOCK_TABLE 
-- (symbol, start_date, end_date, open, close, high, low, volume, indicator)
-- VALUES 
-- ('TSLA', '2025-03-02', '2099-12-31', 700.0, 710.0, 720.0, 690.0, 450000, 1);


-- -- Updating record in Final table 

-- UPDATE MY_DATABASE.MY_SCHEMA.FINAL_STOCK_TABLE t
-- SET end_date = CURRENT_DATE, indicator = 0
-- WHERE indicator = 1
-- AND EXISTS (
--     SELECT 1 FROM MY_DATABASE.MY_SCHEMA.AAPL_STAGING s
--     WHERE t.symbol = s.symbol
--     AND t.end_date = '2099-12-31'
--     AND t.open <> s.close  -- Only update if today's open ≠ yesterday's close
-- );


-- INSERT INTO MY_DATABASE.MY_SCHEMA.FINAL_STOCK_TABLE
-- (symbol, start_date, end_date, open, close, high, low, volume, indicator)
-- SELECT 
--     s.symbol,
--     CURRENT_DATE,  -- Today's start date
--     '2099-12-31',  --  Active till future update
--     t.close,  -- Yesterday's close becomes today's open
--     s.close, 
--     s.high, 
--     s.low, 
--     s.volume,
--     1              --  Active indicator
-- FROM MY_DATABASE.MY_SCHEMA.AAPL_STAGING s
-- JOIN MY_DATABASE.MY_SCHEMA.FINAL_STOCK_TABLE t
-- ON s.symbol = t.symbol
-- WHERE t.end_date = CURRENT_DATE  --  Expired records only
-- LIMIT 1;


-- UPDATE MY_DATABASE.MY_SCHEMA.FINAL_STOCK_TABLE t
-- SET end_date = CURRENT_DATE, indicator = 0
-- WHERE indicator = 1
-- AND symbol = 'TSLA'
-- AND EXISTS (
--     SELECT 1 FROM MY_DATABASE.MY_SCHEMA.TSLA_STAGING s
--     WHERE t.symbol = s.symbol
--     AND t.end_date = '2099-12-31'
--     AND t.open <> s.close  --Only update if today's open ≠ yesterday's close
-- );

-- INSERT INTO MY_DATABASE.MY_SCHEMA.FINAL_STOCK_TABLE
-- (symbol, start_date, end_date, open, close, high, low, volume, indicator)
-- SELECT 
--     s.symbol,
--     CURRENT_DATE,  --  Today's start date
--     '2099-12-31',  -- Active till future update
--     t.close,  -- Yesterday's close becomes today's open
--     s.close, 
--     s.high, 
--     s.low, 
--     s.volume,
--     1              --  Active indicator
-- FROM MY_DATABASE.MY_SCHEMA.TSLA_STAGING s
-- JOIN MY_DATABASE.MY_SCHEMA.FINAL_STOCK_TABLE t
-- ON s.symbol = t.symbol
-- WHERE t.end_date = CURRENT_DATE  -- Expired records only
-- LIMIT 1;



-- Task and Stored Procedure to Automate the Whole thing
CREATE OR REPLACE PROCEDURE MY_DATABASE.MY_SCHEMA.UPDATE_FINAL_STOCK_TABLE()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    --  Expire old AAPL records if the open price has changed
    UPDATE MY_DATABASE.MY_SCHEMA.FINAL_STOCK_TABLE t
    SET end_date = CURRENT_DATE, indicator = 0
    WHERE indicator = 1
    AND symbol = 'AAPL'
    AND EXISTS (
        SELECT 1 FROM MY_DATABASE.MY_SCHEMA.AAPL_STAGING s
        WHERE t.symbol = s.symbol
        AND t.end_date = '2099-12-31'
        AND t.open <> s.close
    );

    -- Expire old TSLA records if the open price has changed
    UPDATE MY_DATABASE.MY_SCHEMA.FINAL_STOCK_TABLE t
    SET end_date = CURRENT_DATE, indicator = 0
    WHERE indicator = 1
    AND symbol = 'TSLA'
    AND EXISTS (
        SELECT 1 FROM MY_DATABASE.MY_SCHEMA.TSLA_STAGING s
        WHERE t.symbol = s.symbol
        AND t.end_date = '2099-12-31'
        AND t.open <> s.close
    );

    --  Insert a new AAPL record
    INSERT INTO MY_DATABASE.MY_SCHEMA.FINAL_STOCK_TABLE
    (symbol, start_date, end_date, open, close, high, low, volume, indicator)
    SELECT 
        s.symbol,
        CURRENT_DATE,
        '2099-12-31',
        t.close,  --  Yesterday's close becomes today's open
        s.close, 
        s.high, 
        s.low, 
        s.volume,
        1
    FROM MY_DATABASE.MY_SCHEMA.AAPL_STAGING s
    JOIN MY_DATABASE.MY_SCHEMA.FINAL_STOCK_TABLE t
    ON s.symbol = t.symbol
    WHERE t.end_date = CURRENT_DATE
    AND t.indicator = 0
    LIMIT 1;

    --  Insert a new TSLA record
    INSERT INTO MY_DATABASE.MY_SCHEMA.FINAL_STOCK_TABLE
    (symbol, start_date, end_date, open, close, high, low, volume, indicator)
    SELECT 
        s.symbol,
        CURRENT_DATE,
        '2099-12-31',
        t.close,  
        s.close, 
        s.high, 
        s.low, 
        s.volume,
        1
    FROM MY_DATABASE.MY_SCHEMA.TSLA_STAGING s
    JOIN MY_DATABASE.MY_SCHEMA.FINAL_STOCK_TABLE t
    ON s.symbol = t.symbol
    WHERE t.end_date = CURRENT_DATE
    AND t.indicator = 0
    LIMIT 1;

    RETURN 'FINAL_STOCK_TABLE Updated Successfully!';
END;
$$;


CREATE OR REPLACE TASK MY_DATABASE.MY_SCHEMA.UPDATE_FINAL_TASK
SCHEDULE = 'USING CRON 0 2 * * * UTC'  -- 🕑 Runs daily at 2 AM UTC
AS
CALL MY_DATABASE.MY_SCHEMA.UPDATE_FINAL_STOCK_TABLE();

ALTER TASK MY_DATABASE.MY_SCHEMA.UPDATE_FINAL_TASK RESUME;


CALL MY_DATABASE.MY_SCHEMA.UPDATE_FINAL_STOCK_TABLE();



select * from final_stock_table;



SHOW TABLES;




DESC TABLE MY_DATABASE.MY_SCHEMA.FINAL_STOCK_TABLE;


SELECT DISTINCT START_DATE FROM MY_DATABASE.MY_SCHEMA.FINAL_STOCK_TABLE ORDER BY START_DATE;









