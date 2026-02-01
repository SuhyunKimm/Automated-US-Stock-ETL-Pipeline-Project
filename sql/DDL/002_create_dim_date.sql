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
	create table analytics.dim_date (
		dateKey int,
		fullDate date not null,
		dayOfMonth int not null,
		dayName varchar(10) not null,
		dayOfWeek int not null,
		dayOfWeekInMonth int not null,
		dayOfWeekInYear int not null,
		dayOfYear int not null,
		weekOfYear int not null,
		monthName varchar(10) not null,
		monthNum int not null,
		quarter int not null,
		year int not null,
		isWeekend bit not null,
		isHoliday bit not null,

		constraint PK_analytics_dim_date primary key (dateKey)
	);
	print 'A table ''analytics.dim_date'' is created.';
end