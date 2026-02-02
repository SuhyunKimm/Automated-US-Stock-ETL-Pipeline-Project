/*
================================================================================
Procedure: clean.sp_Load_us_market_holidays_staging
Purpose  : Load and upsert raw us market holiday data from the bronze layer 
		   into the clean (silver) layer.
Author   : Suhyun Kim
Created  : 2026-02-01
Notes    :
================================================================================
*/

use USStocks;
go

create or alter procedure clean.sp_Load_us_market_holidays_staging
as
begin
	set nocount on;

	merge clean.us_market_holidays as t
	using (
		select
			date = cast([date] as date),
			[status],
			startDateTime = 
				case 
					when startTime is null then null
				else
					dateadd(minute, datediff(minute, '00:00', cast(startTime as time(3))), cast([date] as datetime2(3)))
				end,
			endDateTime = 
				case 
					when endTime is null then null
				else
					dateadd(minute, datediff(minute, '00:00', cast(endTime as time(3))), cast([date] as datetime2(3)))
				end,
			[description]
		from raw.us_market_holidays) 
	as s
	on t.[date] = s.[date]

	when matched and (
		   (isnull(t.[status], '') <> isnull(s.[status], ''))
		or (isnull(t.startTime, '') <> isnull(s.startDateTime, ''))
		or (isnull(t.endTime, '') <> isnull(s.endDateTime, ''))
		or (isnull(t.[description], '') <> isnull(s.[description], '')))
	then
		update set
			t.[status] = s.[status],
			t.startTime = s.startDateTime,
			t.endTime = s.endDateTime,
			t.[description] = s.[description],
			t.lastUpdatedAt = sysdatetime()
	when not matched then
		insert ([date], [status], startTime, endTime, [description], ingestedAt, lastUpdatedAt)
		values (
			s.[date],
			s.[status],
			s.startDateTime,
			s.endDateTime,
			s.[description],
			sysdatetime(),
			sysdatetime()
		);

	print 'Table ''clean.us_market_holidays'' is updated and inserted.';
end;