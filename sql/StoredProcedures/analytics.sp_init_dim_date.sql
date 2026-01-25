/*
================================================================================
Procedure: analytics.sp_init_dim_date
Purpose  : Initializes the dim_date table with date data
           ranging from 2021-01-01 to 2030-12-31.
Author   : Suhyun Kim
Created  : 2026-01-25
Notes    :
  - This procedure is intended to be executed only once.
  - It should be run after the dim_date table is created
    via 002_create_dim_date.sql.
================================================================================
*/

use USStocks;

create or alter proc analytics.sp_init_dim_date
as
begin
	set nocount on;
	
	if exists (select 1 from analytics.dim_date)
	begin
		throw 50001, 'dim_date already contains data. Can''t initialize.', 1;
		return;
	end;

	-- Declare start and end dates for the dimension
	DECLARE @StartDate DATE = '2021-01-01';
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
		(DAY(Date)-1) / 7 + 1 AS DayOfWeekInMonth, 
		DATEPART(WEEKDAY, Date) AS DayOfWeekInYear,
		DATEPART(DAYOFYEAR, Date) AS DayOfYear,
		DATEPART(WEEK, Date) AS WeekOfYear,
		DATENAME(MONTH, Date) AS MonthName,
		MONTH(Date) AS Month,
		DATEPART(QUARTER, Date) AS Quarter,
		YEAR(Date) AS Year,
		CASE
			WHEN DATEPART(WEEKDAY, Date) IN (1, 7) THEN 1
			ELSE 0
		END AS IsWeekend,
		IsHoliday = 0
	FROM
		DateSequence
	OPTION (MAXRECURSION 0); 

	update d
	set IsHoliday = 1
	from analytics.dim_date d
	inner join analytics.us_market_holidays h
	on d.FullDate = h.[Date];
end;