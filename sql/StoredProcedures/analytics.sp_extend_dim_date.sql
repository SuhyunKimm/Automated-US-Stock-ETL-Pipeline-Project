/*
================================================================================
Procedure: analytics.sp_Upsert_dim_date
Purpose  : Update and insert the dim_date table to ensure it always contains
           date records up to 5 years beyond the current date.
Author   : Suhyun Kim
Created  : 2026-01-25
Notes    :
  - This procedure is safe to run repeatedly (idempotent).
  - New dates are inserted only when the current maximum date
    in dim_date is less than (today + 5 years).
  - Intended to be executed as part of a regular ETL pipeline
    before fact data is loaded.
  - This stored procedure also updates 'isHoliday' column values when 'analytics.
	us_market_holidays' table is updated.
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
		@maxDimDate = isnull(max(FullDate), '2021-01-01')
	from analytics.dim_date;

	set 
		@currentDate = cast(getdate() as date);
	set
		@targetEndDate = dateadd(year, 5, @currentDate);
	

	if @maxDimDate < @targetEndDate
	begin
		with DateSequence([Date]) AS (
			select dateadd(day, 1, @maxDimDate) AS [date]
			union all
			select dateadd(day, 1, [date])
			from DateSequence
			where [date] <= @targetEndDate
		)

		insert into analytics.dim_date (
			dateKey,
			fullDate,
			dayOfMonth,
			dayName,
			dayOfWeek,
			dayOfWeekInMonth,
			dayOfWeekInYear,
			dayOfYear,
			weekOfYear,
			monthName,
			monthNum,
			quarter,
			year,
			isWeekend,
			isHoliday
		)
		select
		convert(int, convert(char(8), [Date], 112)) AS dateKey,
		[date] AS fullDate,
		day([Date]) AS [dayOfMonth],
		datename(weekday, [Date]) AS [dayName],
		datepart(weekday, [Date]) AS [dayOfWeek],
		(DAY(Date)-1) / 7 + 1 AS dayOfWeekInMonth, 
		datepart(weekday, [Date]) AS dayOfWeekInYear,
		datepart(dayofyear, [Date]) AS [dayOfYear],
		datepart(week, [Date]) AS weekOfYear,
		datename(month, [Date]) AS [monthName],
		month([Date]) AS monthNum,
		datepart(quarter, [Date]) AS [quarter],
		year([Date]) AS [year],
		case 
			when datename(weekday, [Date]) in ('Saturday','Sunday') then 1 
			else 0 
		end as IsWeekend,
		IsHoliday = 0
		from
			DateSequence
		option (maxrecursion 0); 
		
		print 'Table ''analytics.dim_date'' is extended to ' + convert(varchar(10), @targetEndDate, 120);
	end;

	update d
	set isHoliday = 1
	from analytics.dim_date d
	inner join analytics.us_market_holidays h
	on d.fullDate = h.[date]
	where d.fullDate > @maxDimDate;

end;