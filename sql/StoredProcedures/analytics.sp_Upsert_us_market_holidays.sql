/*
================================================================================
Procedure: analytics.sp_Upsert_us_market_holidays
Purpose  : Load and upsert cleaned us market holiday data from the clean layer 
		   into the analytics (gold) layer.
Author   : Suhyun Kim
Created  : 2026-01-26
Notes    :
  - The table analytics.us_market_holidays must be ready before upserting the
  analytics.dim_date table.
================================================================================
*/

use USStocks;
go

create or alter procedure analytics.sp_Upsert_us_market_holidays
as
begin
	set nocount on;

	merge analytics.us_market_holidays as t
	using (
		select
			[date],
			[status],
			[startTime],
			[endTime],
			[description],
			isTradingDay = 
				case
					when [status] = 'closed' then 0
				else 1
			end,
			isShortDay =
				case
					when [status] = 'short day' then 1
				else 0
			end
		from clean.us_market_holidays
	) as s
	on t.[date] = s.[date]

	when matched and (
		   (isnull(t.[status], '') <> isnull(s.[status], ''))
		or (isnull(t.startTime, '') <> isnull(s.startTime, ''))
		or (isnull(t.endTime, '') <> isnull(s.endTime, ''))
		or (isnull(t.[description], '') <> isnull(s.[description], ''))
		or (isnull(t.isTradingDay, '') <> isnull(s.isTradingDay, ''))
		or (isnull(t.isShortDay, '') <> isnull(t.isShortDay, '')))
	then
		update set
			t.[status] = s.[status],
			t.startTime = s.startTime,
			t.endTime = s.endTime,
			t.[description] = s.[description],
			t.isTradingDay = s.isTradingDay,
			t.isShortDay = s.isShortDay,
			t.lastUpdatedAt = sysdatetime()
	when not matched then
		insert ([date], [status], startTime, endTime, [description], isTradingDay, isShortDay, ingestedAt, lastUpdatedAt)
		values (
			s.[date],
			s.[status],
			s.startTime,
			s.endTime,
			s.[description],
			s.isTradingDay,
			s.isShortDay,
			sysdatetime(),
			sysdatetime()
		);

	print 'Table ''analytics.us_market_holidays'' is updated.';
end;