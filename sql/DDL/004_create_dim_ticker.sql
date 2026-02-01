/*
================================================================================
Script: 004_create_dim_ticker.sql
Purpose: Creates the 'dim_ticker' table which stores the ticker master data.
Author : Suhyun Kim
Created: 2026-01-24
Notes  : 
	- This script creates dim_ticker tables for each 'clean' and 'analytics'
	schema respectively.
	- Clean data is inserted into clean.dim_ticker by the Python script 
	'py_Get_ticker_data'.
	- The table analytics.dim_ticker represents the gold layer and is populated 
	and updated from clean.dim_ticker
================================================================================
*/

use USStocks;

if not exists (
	select * from INFORMATION_SCHEMA.TABLES 
	where
		TABLE_SCHEMA = 'clean'
		and TABLE_NAME = 'dim_ticker'
)
begin
	CREATE TABLE clean.dim_ticker (
		ticker nvarchar(10) not null unique,
		companyName nvarchar(100) null,
		country nvarchar(50) null,
		industry nvarchar(50) null,
		market nvarchar(50) null,
		currency nvarchar(10) null,
		ingestedAt datetime2(3) default sysdatetime()
	);
	print 'A table ''clean.dim_ticker'' is created.';
end

if not exists (
	select * from INFORMATION_SCHEMA.TABLES 
	where
		TABLE_SCHEMA = 'analytics'
		and TABLE_NAME = 'dim_ticker'
)
begin
	CREATE TABLE analytics.dim_ticker (
		tickerId int identity(1,1),
		ticker nvarchar(10) not null unique,
		companyName nvarchar(100) null,
		country nvarchar(50) null,
		industry nvarchar(50) null,
		market nvarchar(50) null,
		currency nvarchar(10) null,
		updatedAt datetime2(3) not null default sysdatetime(),

		constraint PK_analytics_dim_ticker primary key (tickerId)
	);
	print 'A table ''analytics.dim_ticker'' is created.';
end