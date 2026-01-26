/*
================================================================================
Procedure: analytics.sp_Load_us_stocks_staging
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
go

create or alter procedure analytics.sp_Load_us_stocks_staging
AS
begin
	set nocount on;

	begin try
		with data as (
			select
				Ticker,
				try_cast([Date] as datetime2(3)) as [Date],
				try_cast([Open] as decimal(20,6)) as [Open],
				try_cast([High] as decimal(20,6)) as [High],
				try_cast([Low] as decimal(20,6)) as [Low],
				try_cast([Close] as decimal(20,6)) as [Close],
				try_cast(Volume as bigint) as Volume,
				ingested_at,
				row_number() over(
					partition by Ticker, [Date]
					order by ingested_at desc
				) as rn
			from raw.us_stocks
			where [Date] is not null
		)
		merge into clean.us_stocks As t
		using (
			select
				Ticker, [Date], [Open], [High], [Low], [Close], Volume, ingested_at
			from data 
			where rn = 1
		) as s
		on (t.Ticker = s.Ticker and t.[Date] = s.[Date])
		when matched then
			update set
				t.[Open] = s.[Open],
				t.[High] = s.[high],
				t.[Low] = s.[low],
				t.[Close] = s.[Close],
				t.Volume = s.volume,
				t.ingested_at = s.ingested_at
		when not matched by target then
			insert (Ticker, [Date], [Open], [High], [Low], [Close], Volume, ingested_at)
			values (s.Ticker, s.[Date], s.[Open], s.[High], s.[Low], s.[Close], s.Volume, s.ingested_at);

		print 'Table ''clean.us_stocks'' is loaded and upserted.';

	end try
	begin catch
		print 'Error occurred in Staging Layer Load'
	end catch
end;