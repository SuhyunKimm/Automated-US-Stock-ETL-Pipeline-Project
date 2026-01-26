/*
================================================================================
Script: 001_create_us_market_holidays.sql
Purpose: Creates the 'us_market_holidays' table which stores the us market holiday information.
Author : Suhyun Kim
Created: 2026-01-24
================================================================================
*/
use USStocks;

if not exists (
	select * from INFORMATION_SCHEMA.TABLES 
	where
		TABLE_SCHEMA = 'clean'
		and TABLE_NAME = 'us_market_holidays'
)
begin
	create table clean.us_market_holidays (
		[Date] date not null unique,
		[Status] nvarchar(20),
		Start_Time varchar(20) null,
		End_Time varchar(20) null,
		[Description] nvarchar(100),
		ingested_at datetime2(3) default sysdatetime()
	);
end

if not exists (
	select * from INFORMATION_SCHEMA.TABLES 
	where
		TABLE_SCHEMA = 'analytics'
		and TABLE_NAME = 'us_market_holidays'
)
begin
	create table analytics.us_market_holidays (
		[Date] date primary key,
		[Status] nvarchar(20),
		Start_Time varchar(20) null,
		End_Time varchar(20) null,
		[Description] nvarchar(100),
		ingested_at datetime2(3) default sysdatetime()
	);
end