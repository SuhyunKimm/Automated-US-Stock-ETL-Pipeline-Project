/*
================================================================================
Script   : 003_create_us_stocks.sql
Purpose  : 
	Creates 'raw.us_stocks' (raw data) and 'clean.us_stocks' (staging for 
	transformations) tables.
Author   : Suhyun Kim
Created  : 2026-01-24
Notes    : 
  - 'raw.us_stocks' stores all raw data as NVARCHAR for initial ingestion.
  - 'clean.us_stocks' stores typed and transformed data ready for analytics.
================================================================================
*/

-- Bronze Layer : Raw 
use USStocks;

if not exists (
	select * from INFORMATION_SCHEMA.TABLES 
	where
		TABLE_SCHEMA = 'raw'
		and TABLE_NAME = 'us_stocks'
)
begin
	create table raw.us_stocks (
		Ticker nvarchar(50),
		[Date] nvarchar(50),
		[Open] nvarchar(50),
		[High] nvarchar(50),
		[Low] nvarchar(50),
		[Close] nvarchar(50),
		Volume nvarchar(50),
		ingested_at datetime2(3) default getdate()
	);
	print 'A table ''raw.us_stocks'' is created.';
end

if not exists (
	select * from INFORMATION_SCHEMA.TABLES 
	where
		TABLE_SCHEMA = 'clean'
		and TABLE_NAME = 'us_stocks'
)
begin
	create table clean.us_stocks (
		Ticker nvarchar(10) not null,
		[Date] date not null,
		[Open] decimal(20,6),
		[High] decimal(20,6),
		[Low] decimal(20,6),
		[Close] decimal(20,6),
		Volume bigint,
		ingested_at datetime2(3) default getdate(),

		constraint pk_clean_us_stocks primary key (Ticker, [Date])
	);
	print 'A table ''clean.us_stocks'' is created.';
end
