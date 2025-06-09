-- First, create the calendar table
CREATE TABLE calendar_dim (
    date_id DATE PRIMARY KEY,
    week_of_year INTEGER,
    day_of_year INTEGER,
    day_of_week TEXT,
    month_num INTEGER,
    day_type TEXT,
    month_name TEXT,
    month_short TEXT,
    quarter INTEGER,
    year INTEGER,
    year_quarter TEXT,
    month_year TEXT
);

-- Then, populate it with data using min/max dates from database_cleaned
INSERT INTO calendar_dim
WITH RECURSIVE dates AS (
    SELECT 
        (SELECT DATE(MIN(time_bucket)) FROM database_cleaned) AS date_id
    UNION ALL
    SELECT date_id + 1
    FROM dates
    WHERE date_id < (SELECT DATE(MAX(time_bucket)) FROM database_cleaned)
)
SELECT 
    date_id,
    EXTRACT(WEEK FROM date_id) AS week_of_year,
    EXTRACT(DOY FROM date_id) AS day_of_year,
    TO_CHAR(date_id, 'Day') AS day_of_week,
    EXTRACT(MONTH FROM date_id) AS month_num,
    CASE 
        WHEN EXTRACT(DOW FROM date_id) IN (0, 6) THEN 'Weekend' 
        ELSE 'Weekday' 
    END AS day_type,
    TO_CHAR(date_id, 'Month') AS month_name,
    TO_CHAR(date_id, 'Mon') AS month_short,
    EXTRACT(QUARTER FROM date_id) AS quarter,
    EXTRACT(YEAR FROM date_id) AS year,
    EXTRACT(YEAR FROM date_id)::TEXT || '-Q' || EXTRACT(QUARTER FROM date_id)::TEXT AS year_quarter,
    TO_CHAR(date_id, 'Mon YYYY') AS month_year
FROM dates;

-- Create indexes for better performance
CREATE INDEX idx_calendar_year ON calendar_dim(year);
CREATE INDEX idx_calendar_month ON calendar_dim(month_num);
CREATE INDEX idx_calendar_quarter ON calendar_dim(quarter);