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

	;with DateSequence([date]) as (
		select @StartDate as [date]
		union all
		select dateadd(day, 1, [date])
		from DateSequence
		where [date] < @EndDate
	)

	insert into analytics.dim_date (
		dateKey,
		fullDate,
		[dayOfMonth],
		[dayName],
		[dayOfWeek],
		dayOfWeekInMonth,
		dayOfWeekInYear,
		[dayOfYear],
		weekOfYear,
		[monthName],
		monthNum,
		[quarter],
		[year],
		isWeekend,
		isHoliday
	)
	select
		convert(int, convert(char(8), [date], 112)) AS dateKey,
		[date] AS fullDate,
		day([date]) AS [dayOfMonth],
		datename(weekday, [date]) AS [dayName],
		datepart(weekday, [date]) AS [dayOfWeek],
		(DAY(Date)-1) / 7 + 1 AS dayOfWeekInMonth, 
		datepart(weekday, [date]) AS dayOfWeekInYear,
		datepart(dayofyear, [date]) AS [dayOfYear],
		datepart(week, [date]) AS weekOfYear,
		datename(month, [date]) AS [monthName],
		month([date]) AS monthNum,
		datepart(quarter, [date]) AS [quarter],
		year([date]) AS [year],
		case 
			when datename(weekday, [date]) in ('Saturday','Sunday') then 1 
			else 0 
		end as isWeekend,
		isHoliday = 0
	from
		DateSequence
	option (maxrecursion 0); 

	update d
	set isHoliday = 1
	from analytics.dim_date d
	inner join analytics.us_market_holidays h
	on d.fullDate = h.[date];

	print 'Table ''analytics.dim_date'' is populated.';
end;