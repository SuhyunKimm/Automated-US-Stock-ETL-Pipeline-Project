/*
================================================================================
Script: 006_create_etl_last_loaded.sql
Purpose: 
	Creates the 'etl_last_loaded' table, which stores the last loaded date for
	the ETL pipeline.
Author : Suhyun Kim
Created: 2026-01-31
Notes  : 
	- The column 'last_loaded_date' stores the most recent date of the data
	loaded by the pipeline.
	- The column 'updated_at' stores the timestamp when this table was last
	updated.
	- The column 'updated_at' is based on the time when each table in the raw
	layer is populated.
================================================================================
*/

use USStocks;

if not exists (
	select * from INFORMATION_SCHEMA.TABLES 
	where
		TABLE_SCHEMA = 'analytics'
		and TABLE_NAME = 'etl_last_loaded'
)
begin
	create table analytics.etl_last_loaded (
		tableName varchar(50),
		lastLoadedDate date,
		updatedAt datetime2(3) not null default sysdatetime(),

		constraint PK_etl_last_loaded primary key (tableName)
	);
	print 'A table ''analytics.etl_last_loaded'' is created.';
end