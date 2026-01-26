/*
================================================================================
Procedure: analytics.sp_Extend_dim_date
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
go

create or alter proc analytics.sp_Extend_dim_date
as
begin
	set nocount on;

	-- Declare start and end dates for the dimension
	declare @maxDimDate date;
	declare @targetEndDate date;
	declare @currentDate date;

	select 
		@maxDimDate = max(FullDate) 
	from analytics.dim_date;

	set 
		@currentDate = cast(getdate() as date);
	set
		@targetEndDate = dateadd(year, 5, @currentDate);
	
	if @maxDimDate is null
	begin
		throw 50002, 'dim_date is empty. Run sp_init_dim_date first.', 1;
	end;

	if @maxDimDate < @targetEndDate
	begin
		with DateSequence([Date]) AS (
			select dateadd(day, 1, @maxDimDate) AS [date]
			union all
			select dateadd(day, 1, [date])
			FROM DateSequence
			WHERE [date] <= @targetEndDate
		)

		insert into analytics.dim_date (
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
		select
		convert(int, convert(char(8), [Date], 112)) AS DateKey,
		[Date] AS FullDate,
		day([Date]) AS DayOfMonth,
		datename(weekday, [Date]) AS DayName,
		datepart(weekday, [Date]) AS DayOfWeek,
		(DAY(Date)-1) / 7 + 1 AS DayOfWeekInMonth, 
		datepart(weekday, [Date]) AS DayOfWeekInYear,
		datepart(dayofyear, [Date]) AS DayOfYear,
		datepart(week, [Date]) AS WeekOfYear,
		datename(month, [Date]) AS MonthName,
		month([Date]) AS Month,
		datepart(quarter, [Date]) AS Quarter,
		year([Date]) AS Year,
		case 
			when datename(weekday, [Date]) in ('Saturday','Sunday') then 1 
			else 0 
		end as IsWeekend,
		IsHoliday = 0
		from
			DateSequence
		option (maxrecursion 0); 

		update d
		set IsHoliday = 1
		from analytics.dim_date d
		inner join analytics.us_market_holidays h
		on d.FullDate = h.[Date]
		where d.FullDate > @maxDimDate;
		
		print 'Table ''analytics.dim_date'' is extended to ' + convert(varchar(10), @targetEndDate, 120);
	end;
end;