/*
================================================================================
Script: 000_create_schemas.sql
Purpose:¡¡Create database 'USStocks' and schemas 'raw', 'clean', and 'analytics' 
		¡¡for the Bronze, Silver, and Gold layers, respectively.
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
	print 'A database ''USStocks'' is created.';
end
go

-- Use the database
use USStocks;
go

-- Bronze Layer
if not exists (select * from sys.schemas where name = 'raw')
begin
	exec('create schema raw authorization dbo;');
	print 'A schema ''raw'' is created.';
end
go

-- Silver Layer
if not exists (select * from sys.schemas where name = 'clean')
begin
	exec('create schema clean authorization dbo;');
	print 'A schema ''clean'' is created.';
end
go

-- Gold Layer
if not exists (select * from sys.schemas where name = 'analytics')
begin
	exec('create schema analytics authorization dbo;');
	print 'A schema ''analytics'' is created.';
end
go
