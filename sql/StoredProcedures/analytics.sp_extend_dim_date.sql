/*
================================================================================
Procedure: analytics.sp_extend_dim_date
Purpose  : Extends the dim_date table to ensure it always contains
           date records up to 5 years beyond the current date.
Author   : Suhyun Kim
Created  : 2026-01-25
Notes    :
  - This procedure is safe to run repeatedly (idempotent).
  - New dates are inserted only when the current maximum date
    in dim_date is less than (today + 5 years).
  - Intended to be executed as part of a regular ETL pipeline
    before fact data is loaded.
================================================================================
*/

use USStocks;

create or alter proc analytics.sp_extend_dim_date
as
begin
	set nocount on;

	-- Declare start and end dates for the dimension
	declare @maxDimDate DATE;
	declare @targetEndDate DATE;
	declare @currentDate DATE;

	select 
		@maxDimDate = max(FullDate) 
	from analytics.dim_date;

	set 
		@currentDate = cast(GETDATE() as DATE);
	set
		@targetEndDate = dateadd(year, 5, @currentDate);
	
	if @maxDimDate is null
	begin
		throw 50002, 'dim_date is empty. Run sp_init_dim_date first.', 1;
	end;

	if @maxDimDate < @targetEndDate
	begin
		
		-- A Common Table Expression (CTE) to generate a sequence of dates
		;WITH DateSequence(Date) AS (
			SELECT dateadd(day, 1, @maxDimDate) AS Date
			UNION ALL
			SELECT DATEADD(DAY, 1, Date)
			FROM DateSequence
			WHERE Date <= @targetEndDate
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
		on d.FullDate = h.[Date]
		where d.FullDate > @maxDimDate;
		
		print 'dim_date table extended to ' + @targetEndDate;
	end;
end;