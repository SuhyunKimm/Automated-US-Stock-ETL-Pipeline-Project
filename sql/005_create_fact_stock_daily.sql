/*
================================================================================
Script: 005_create_fact_stock_daily.sql
Purpose: Creates the 'fact_stock_daily' table which stores
Author : Suhyun Kim
Created: 2026-01-24
Notes  : 
================================================================================
*/

CREATE TABLE analytics.fact_stock_daily (
    date_key INT REFERENCES analytics.dim_date(date_key),
    ticker_key INT REFERENCES analytics.dim_ticker(ticker_key),
    open_price FLOAT,
    high_price FLOAT,
    low_price FLOAT,
    close_price FLOAT,
    volume BIGINT,
    daily_return FLOAT,
    cumulative_return FLOAT,
    volatility_20d FLOAT,
    ingested_at DATETIME2(3) DEFAULT GETDATE(),
    PRIMARY KEY (date_key, ticker_key)
);