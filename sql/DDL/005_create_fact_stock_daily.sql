/*
================================================================================
Script: 005_create_fact_stock_daily.sql
Purpose: 
	Creates the 'fact_stock_daily' table, which stores daily transformed stock 
	data derived from the 'clean.us_stocks' table.
Author : Suhyun Kim
Created: 2026-01-24
Notes  : 
	- This table represents the final (gold) layer and is used as the primary
	data source for Power BI reporting and analytics.
================================================================================
*/

use USStocks;

if not exists (
	select * from INFORMATION_SCHEMA.TABLES 
	where
		TABLE_SCHEMA = 'analytics'
		and TABLE_NAME = 'fact_stock_daily'
)
begin
	CREATE TABLE analytics.fact_stock_daily (
		dateKey int not null,
		tickerId int not null,
		open_price decimal(20,6) not null,
		high_price decimal(20,6) not null,
		low_price decimal(20,6) not null,
		close_price decimal(20,6) not null,
		volume bigint not null,
		daily_return decimal(20,6) not null,
		cumulative_return decimal(20,6) not null,
		volatility_20d decimal(20,6) not null,
		updated_at datetime2(3) not null default sysdatetime(),

		constraint PK_fact_stock_daily 
			primary key (DateKey, tickerId),
		constraint FK_fact_stock_daily_DateKey 
			foreign key (DateKey) references analytics.dim_date(DateKey),
		constraint FK_fact_stock_daily_tickerId 
			foreign key (tickerId) references analytics.dim_ticker(tickerId)
	);
	print 'A table ''analytics.fact_stock_daily'' is created.';
end
