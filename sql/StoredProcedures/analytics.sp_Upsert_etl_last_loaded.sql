/*
================================================================================
Procedure: analytics.sp_upsert_etl_last_loaded
Purpose  : 
	Upserts the last successfully loaded date for a given table into 
	analytics.etl_last_loaded.
Author   : Suhyun Kim
Created  : 2026-01-31
Notes    :
================================================================================
*/

use USStocks;
go

create or alter procedure analytics.sp_upsert_etl_last_loaded
	@tableName varchar(50),
	@last_loaded_date date

as
begin
	set nocount on;

	begin try
		if exists (select 1 from analytics.etl_last_loaded where tableName = @tableName and last_loaded_date <> @last_loaded_date)
		begin
			update analytics.etl_last_loaded
			set 
				last_loaded_date = @last_loaded_date,
				updated_at = sysdatetime()
			where 
				tableName = @tableName
		end
		else if not exists (select 1 from analytics.etl_last_loaded where tableName = @tableName)
		begin
			insert into analytics.etl_last_loaded(tableName, last_loaded_date, updated_at)
			values (@tableName, @last_loaded_date, sysdatetime());
		end
		else
		begin
			print '';
		end
		print 'Table ''analytics.etl_last_loaded'' is updated or inserted.';
end try
begin catch
	throw;
end catch
end;