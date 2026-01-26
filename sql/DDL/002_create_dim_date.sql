/*
================================================================================
Script: 002_create_dim_date.sql
Purpose: Creates the 'dim_date' table which stores the date dimension.
Author : Suhyun Kim
Created: 2026-01-24
Notes  : 
	- This table is intended to be static, but it may be updated with additional
	dates when new data with more recent dates is inserted in the us_stock table.
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
		dateKey int,
		Fulldate date not null,
		DayOfMonth int not null,
		DayName varchar(10) not null,
		DayOfWeek int not null,
		DayOfWeekInMonth int not null,
		DayOfWeekInYear int not null,
		DayOfYear int not null,
		WeekOfYear int not null,
		MonthName varchar(10) not null,
		Month int not null,
		Quarter int not null,
		Year int not null,
		IsWeekend bit not null,
		IsHoliday bit not null,

		constraint PK_analytics_dim_date primary key (dateKey)
	);
	print 'A table ''analytics.dim_date'' is created.';
end