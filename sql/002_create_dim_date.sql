/*
================================================================================
Script: 002_create_dim_date.sql
Purpose: Creates the 'dim_date' table which stores the date dimension.
Author : Suhyun Kim
Created: 2026-01-24
Notes  : This table is static and used as a conformed dimension for fact tables.
================================================================================
*/

use USStocks;
if not exists (
	select * from INFORMATION_SCHEMA.TABLES 
	where
		TABLE_SCHEMA = 'analytics'
		and TABLE_NAME = 'dim_date'
)
begin
	CREATE TABLE analytics.dim_date (
		DateKey INT PRIMARY KEY,
		FullDate DATE NOT NULL,
		DayOfMonth INT NOT NULL,
		DayName VARCHAR(10) NOT NULL,
		DayOfWeek INT NOT NULL,
		DayOfWeekInMonth INT NOT NULL,
		DayOfWeekInYear INT NOT NULL,
		DayOfYear INT NOT NULL,
		WeekOfYear INT NOT NULL,
		MonthName VARCHAR(10) NOT NULL,
		Month INT NOT NULL,
		Quarter INT NOT NULL,
		Year INT NOT NULL,
		IsWeekend BIT NOT NULL,
		IsHoliday BIT NOT NULL
	);
end
-- Declare start and end dates for the dimension
DECLARE @StartDate DATE = '2020-01-01';
DECLARE @EndDate DATE = '2030-12-31';

-- A Common Table Expression (CTE) to generate a sequence of dates
;WITH DateSequence(Date) AS (
    SELECT @StartDate AS Date
    UNION ALL
    SELECT DATEADD(DAY, 1, Date)
    FROM DateSequence
    WHERE Date < @EndDate
)
-- Insert data into the DimDate table from the DateSequence
INSERT INTO analytics.dim_date (
    DateKey,
    FullDate,
    DayOfMonth,
    DayName,
    DayOfWeek,
    DayOfWeekInMonth,
    DayOfWeekInYear,
    DayOfYear,
    WeekOfYear,
    MonthName,
    Month,
    Quarter,
    Year,
    IsWeekend,
    IsHoliday
)
SELECT
    CONVERT(INT, CONVERT(CHAR(8), Date, 112)) AS DateKey,
    Date AS FullDate,
    DAY(Date) AS DayOfMonth,
    DATENAME(WEEKDAY, Date) AS DayName,
    DATEPART(WEEKDAY, Date) AS DayOfWeek,
    DAY(Date) / 7 + 1 AS DayOfWeekInMonth, 
    DATEPART(WEEKDAY, Date) AS DayOfWeekInYear,
    DATENAME(DAYOFYEAR, Date) AS DayOfYear,
    DATEPART(WEEK, Date) AS WeekOfYear,
    DATENAME(MONTH, Date) AS MonthName,
    MONTH(Date) AS Month,
    DATEPART(QUARTER, Date) AS Quarter,
    YEAR(Date) AS Year,
    CASE WHEN DATENAME(WEEKDAY, Date) IN ('Saturday', 'Sunday') THEN 1 ELSE 0 END AS IsWeekend,
    IsHoliday = 0
FROM
    DateSequence
OPTION (MAXRECURSION 0); 

update analytics.dim_date
set IsHoliday = 1
where FullDate in (select [Date] from analytics.us_market_holidays);