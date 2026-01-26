/*
================================================================================
Procedure: analytics.sp_Upsert_us_market_holidays
Purpose  : Load and upsert cleaned us market holiday data from the clean layer 
		   into the analytics (gold) layer.
           - Upsert data into analytics.us_market_holidays using MERGE
Author   : Suhyun Kim
Created  : 2026-01-26
Notes    :
  - 
================================================================================
*/

use USStocks;
go

create or alter procedure analytics.sp_Upsert_us_market_holidays
AS
begin
	set nocount on;

	merge analytics.us_market_holidays as t
	using clean.us_market_holidays as s
	on t.Date = s.Date

	when matched then
		update set
			t.Status = s.Status,
			t.Start_Time = s.Start_Time,
			t.End_Time = s.End_Time,
			t.Description = s.Description,
			t.ingested_at = sysdatetime()
	when not matched then
		insert (Date, Status, Start_Time, End_Time, Description, ingested_at)
		values (
			s.Date,
			s.Status,
			s.Start_Time,
			s.End_Time,
			s.Description,
			sysdatetime()
		);
end;