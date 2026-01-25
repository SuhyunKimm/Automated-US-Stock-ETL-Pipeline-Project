/*
================================================================================
Procedure: analytics.sp_Load_Staging_Layer
Purpose  : Load and upsert cleaned stock price data from the raw layer into the
           staging (silver) layer.
           - Cast raw NVARCHAR fields into proper data types
           - Deduplicate records using the latest ingested_at timestamp
           - Upsert data into stg_us_stocks using MERGE
Author   : Suhyun Kim
Created  : 2026-01-24
Notes    :
  - This procedure represents the Silver layer in the Medallion architecture.
  - Only the most recent record per (Ticker, Date) is retained.
  - TRY_CAST is used to prevent the load from failing due to malformed raw data.
================================================================================
*/

use USStocks;

-- Silver Layer : Staging

create or alter procedure analytics.sp_Load_Staging_Layer
AS
begin
	set nocount on;

	begin try
		with data as (
			select
				Ticker,
				try_cast([Date] as datetime2(3)) as [Date],
				try_cast([Open] as decimal(20,6)) as [Open],
				try_cast([high] as decimal(20,6)) as [high],
				try_cast([low] as decimal(20,6)) as [low],
				try_cast([Close] as decimal(20,6)) as [Close],
				try_cast(volume as bigint) as volume,
				ingested_at,
				row_number() over(
					partition by Ticker, [Date]
					order by ingested_at desc
				) as rn
			from raw_us_stocks
			where [Date] is not null
		)
		merge into stg_us_stocks As t
		using (
			select
				Ticker, [Date], [Open], [high], [low], [Close], volume, ingested_at
			from data 
			where rn = 1
		) as s
		on (t.Ticker = s.Ticker and t.[Date] = s.[Date])
		when matched then
			update set
				t.[Open] = s.[Open],
				t.[high] = s.[high],
				t.[low] = s.[low],
				t.[Close] = s.[Close],
				t.volume = s.volume,
				t.ingested_at = s.ingested_at
		when not matched by target then
			insert (Ticker, [Date], [Open], [high], [low], [Close], volume, ingested_at)
			values (s.Ticker, s.[Date], s.[Open], s.[high], s.[low], s.[Close], s.volume, s.ingested_at);
	end try
	begin catch
		print 'Error occurred in Staging Layer Load'
	end catch
end;