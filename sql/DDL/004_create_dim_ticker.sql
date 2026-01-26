/*
================================================================================
Script: 004_create_dim_ticker.sql
Purpose: Creates the 'dim_ticker' table which stores the ticker master data.
Author : Suhyun Kim
Created: 2026-01-24
Notes  : 
	- This script creates dim_ticker tables for each 'clean' and 'analytics'
	schema respectively.
	- clean.dim_ticker table is to store inserted data and update + insert the
	data into analytics.dim_ticker
================================================================================
*/

use USStocks;

CREATE TABLE clean.dim_ticker (
    ticker NVARCHAR(10) NOT NULL UNIQUE,
    company_name NVARCHAR(100) NULL,
	country nvarchar(50) NULL,
    industry NVARCHAR(50) NULL,
    market NVARCHAR(50) NULL,
    currency NVARCHAR(10) NULL,
	ingested_at datetime2(3) DEFAULT sysdatetime()
);

CREATE TABLE analytics.dim_ticker (
    tickerId INT IDENTITY PRIMARY KEY,
    ticker NVARCHAR(10) NOT NULL UNIQUE,
    company_name NVARCHAR(100) NULL,
	country NVARCHAR(50) NULL,
    industry NVARCHAR(50) NULL,
    market NVARCHAR(50) NULL,
    currency NVARCHAR(10) NULL,
	updated_at datetime2(3)
);