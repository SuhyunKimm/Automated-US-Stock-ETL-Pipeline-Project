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