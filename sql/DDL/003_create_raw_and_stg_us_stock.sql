/*
================================================================================
Script   : 003_create_raw_and_stg_us_stock.sql
Purpose  : Creates 'raw_us_stocks' (raw data) and 'stg_us_stocks' (staging for transformations) tables.
Author   : Suhyun Kim
Created  : 2026-01-24
Notes    : 
  - 'raw_us_stocks' stores all raw data as NVARCHAR for initial ingestion.
  - 'stg_us_stocks' stores typed and transformed data ready for analytics.
================================================================================
*/

-- Bronze Layer : Raw 
use USStocks;

drop table if exists raw.raw_us_stocks;

create table raw.raw_us_stocks (
	Ticker nvarchar(50),
	Date nvarchar(50),
	[Open] nvarchar(50),
	high nvarchar(50),
	low nvarchar(50),
	[Close] nvarchar(50),
	volume nvarchar(50),
	ingested_at datetime2(3) default getdate()
);
print 'Table ''raw_us_stocks'' has been created.';

drop table if exists clean.stg_us_stocks;

create table clean.stg_us_stocks (
	Ticker nvarchar(10) not null,
	Date datetime2(3) not null,
	[Open] decimal(20,6),
	[high] decimal(20,6),
	[low] decimal(20,6),
	[Close] decimal(20,6),
	volume bigint,
	ingested_at datetime2(3) default getdate()
);
print 'Table ''stg_us_stocks'' has been created.';

ALTER TABLE clean.stg_us_stocks 
ADD CONSTRAINT PK_stg_us_stocks PRIMARY KEY (Ticker, Date);
