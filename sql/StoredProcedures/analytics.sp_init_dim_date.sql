/*
================================================================================
Procedure: analytics.sp_Init_dim_date
Purpose  : Initializes the dim_date table with date data
           ranging from 2021-01-01 to 2030-12-31.
Author   : Suhyun Kim
Created  : 2026-01-25
Notes    :
  - This procedure is intended to be executed only once.
  - It should be run after the dim_date table is created
    via 002_create_dim_date.sql and analytics.us_market_holidays table is ready.
================================================================================
*/

use USStocks;
go

create or alter proc analytics.sp_Init_dim_date
as
begin
	set nocount on;
	
	if exists (select 1 from analytics.dim_date)
	begin
		throw 50001, 'analytics.dim_date already contains data. Can''t initialize.', 1;
		return;
	end;

	declare @StartDate date = '2021-01-01';
	declare @EndDate date = '2030-12-31';

	;with DateSequence([Date]) as (
		select @StartDate as [date]
		union all
		select dateadd(day, 1, [date])
		from DateSequence
		where [date] < @EndDate
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
	on d.FullDate = h.[Date];

	print 'Table ''analytics.dim_date'' is populated.';
end;