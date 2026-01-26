/*
================================================================================
Script: 005_create_fact_stock_daily.sql
Purpose: Creates the 'fact_stock_daily' table which stores
Author : Suhyun Kim
Created: 2026-01-24
Notes  : 
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
    DateKey INT NOT NULL REFERENCES analytics.dim_date(DateKey),
    tickerId INT NOT NULL REFERENCES analytics.dim_ticker(tickerId),
    open_price FLOAT,
    high_price FLOAT,
    low_price FLOAT,
    close_price FLOAT,
    volume BIGINT,
    daily_return FLOAT,
    cumulative_return FLOAT,
    volatility_20d FLOAT,
    updated_at DATETIME2(3) DEFAULT sysdatetime(),

	CONSTRAINT PK_fact_stock_daily 
		PRIMARY KEY (DateKey, tickerId),
	CONSTRAINT FK_fact_stock_daily_DateKey 
		FOREIGN KEY (DateKey) REFERENCES analytics.dim_date(DateKey),
	CONSTRAINT FK_fact_stock_daily_tickerId 
		FOREIGN KEY (tickerId) REFERENCES analytics.dim_ticker(tickerId)
);
end;
