/*
================================================================================
Procedure: analytics.sp_Upsert_etl_last_loaded
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

create or alter procedure analytics.sp_Upsert_etl_last_loaded
	@tableName varchar(50),
	@lastLoadedDate date

as
begin
	set nocount on;

	begin try
		if not exists (select 1 from analytics.etl_last_loaded where tableName = @tableName)
		begin
			insert into analytics.etl_last_loaded(tableName, lastLoadedDate, updatedAt)
			values (@tableName, @lastLoadedDate, sysdatetime());
		end
		else if exists (select 1 from analytics.etl_last_loaded where tableName = @tableName and lastLoadedDate <> @lastLoadedDate)
		begin
			update analytics.etl_last_loaded
			set 
				lastLoadedDate = @lastLoadedDate,
				updatedAt = sysdatetime()
			where 
				tableName = @tableName
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