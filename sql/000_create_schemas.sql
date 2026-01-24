/*
================================================================================
Script: 000_create_schemas.sql
Purpose: Create database 'USStocks' and schemas 'raw', 'clean', and 'analytics' for the Bronze, Silver, and Gold layers, respectively.
Author : Suhyun Kim
Created: 2026-01-24
Notes  : Schemas are currently owned by 'dbo'.
		 Permissions for ETL or BI users may need to be granted separately.
================================================================================
*/

-- Create database if not exists
if not exists (select * from sys.databases where name = 'USStocks')
begin
	create database USStocks;
end
go

-- Use the database
use USStocks;
go

-- Bronze Layer
if not exists (select * from sys.schemas where name = 'raw')
	exec('create schema raw authorization dbo;');
go

-- Silver Layer
if not exists (select * from sys.schemas where name = 'clean')
	exec('create schema clean authorization dbo;');
go

-- Gold Layer
if not exists (select * from sys.schemas where name = 'analytics')
	exec('create schema analytics authorization dbo;');
go
