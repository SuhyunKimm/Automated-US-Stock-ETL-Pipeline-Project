/*
================================================================================
Script: 001_create_us_market_holidays.sql
Purpose: Creates the 'us_market_holidays' table which stores the us market 
		 holiday information.
Author : Suhyun Kim
Created: 2026-01-24
Note :
	- 'us_market_holidays' table consists of the following columns :
	[Date, Status, Start_Time, End_Time, Description, ingested_at]
	- Original data is inserted into raw.us_market_holidays by the Python script
	'000_Init_Raw_USStock_Market_Holidays.py'
	- The table clean.us_market_holidays represents the silver layer and is
	populated and updated from raw.us_market_holidays
	- The table analytics.us_market_holidays represents the gold layer
	and is populated and updated from clean.us_market_holidays
================================================================================
*/

use USStocks;

if not exists (
	select * from INFORMATION_SCHEMA.TABLES 
	where
		TABLE_SCHEMA = 'raw'
		and TABLE_NAME = 'us_market_holidays'
)
begin	   
	create table raw.us_market_holidays (
		[date] nvarchar(50) not null,
		[status] nvarchar(50),
		startTime nvarchar(50),
		endTime nvarchar(50),
		[description] nvarchar(200),
		ingestedAt datetime2(3) default sysdatetime()
	);
	print 'A table ''raw.us_market_holidays'' is created.';
end

if not exists (
	select * from INFORMATION_SCHEMA.TABLES 
	where
		TABLE_SCHEMA = 'clean'
		and TABLE_NAME = 'us_market_holidays'
)
begin
	create table clean.us_market_holidays (
		[date] date not null unique,
		[status] nvarchar(20),
		startTime datetime2(3) null,
		endTime datetime2(3) null,
		[description] nvarchar(100),
		ingestedAt datetime2(3) default sysdatetime(),
		lastUpdatedAt datetime2(3) default sysdatetime()
	);
	print 'A table ''clean.us_market_holidays'' is created.';
end

if not exists (
	select * from INFORMATION_SCHEMA.TABLES 
	where
		TABLE_SCHEMA = 'analytics'
		and TABLE_NAME = 'us_market_holidays'
)
begin
	create table analytics.us_market_holidays (
		[date] date primary key,
		[status] nvarchar(20),
		startTime datetime2(3) null,
		endTime datetime2(3) null,
		[description] nvarchar(100),
		isTradingDay bit,
		isShortDay bit,
		ingestedAt datetime2(3) not null default sysdatetime(),
		lastUpdatedAt datetime2(3) default sysdatetime()
	);
	print 'A table ''analytics.us_market_holidays'' is created.';
end