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
		ticker nvarchar(50), 
		[date] nvarchar(50),
		[open] nvarchar(50),
		[high] nvarchar(50),
		[low] nvarchar(50),
		[close] nvarchar(50),
		volume nvarchar(50),
		ingestedAt datetime2(3) default getdate()
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
		ticker nvarchar(10) not null,
		[date] date not null,
		[open] decimal(20,6),
		[high] decimal(20,6),
		[low] decimal(20,6),
		[close] decimal(20,6),
		volume bigint,
		ingestedAt datetime2(3) default getdate(),

		constraint PK_clean_us_stocks primary key (ticker, [date])
	);
	print 'A table ''clean.us_stocks'' is created.';
end
