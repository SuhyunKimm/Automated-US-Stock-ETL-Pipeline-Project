/*
================================================================================
Script: 004_create_dim_ticker.sql
Purpose: Creates the 'dim_ticker' table which stores the ticker master data.
Author : Suhyun Kim
Created: 2026-01-24
Notes  : 
	- This table is static and used as a conformed dimension for fact tables.
	- New tickers are added only when new symbols are introduced.
================================================================================
*/

use USStocks;

CREATE TABLE analytics.dim_ticker (
    tickerId INT IDENTITY PRIMARY KEY,
    ticker NVARCHAR(10),
    company_name NVARCHAR(100) NULL,
    sector NVARCHAR(50) NULL,
    industry NVARCHAR(50) NULL,
    market NVARCHAR(20) NULL,
    currency NVARCHAR(10) NULL
);