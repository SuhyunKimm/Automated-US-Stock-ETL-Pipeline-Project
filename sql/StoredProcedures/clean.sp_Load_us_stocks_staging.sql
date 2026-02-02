/*
================================================================================
Procedure: clean.sp_Load_us_stocks_staging
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

create or alter procedure clean.sp_Load_us_stocks_staging
AS
begin
	set nocount on;

	begin try
		with data as (
			select
				ticker,
				try_cast([date] as datetime2(3)) as [date],
				try_cast([open] as decimal(20,6)) as [open],
				try_cast([high] as decimal(20,6)) as [high],
				try_cast([low] as decimal(20,6)) as [low],
				try_cast([close] as decimal(20,6)) as [close],
				try_cast(volume as bigint) as volume,
				row_number() over(
					partition by ticker, [date]
					order by ingestedAt desc
				) as rn
			from raw.us_stocks
			where [date] is not null
		)
		merge into clean.us_stocks As t
		using (
			select
				ticker, [date], [open], [high], [low], [close], volume
			from data 
			where rn = 1
		) as s
		on (t.ticker = s.ticker and t.[date] = s.[date])
		when matched and 
			((isnull(t.[open], '') <> isnull(s.[open], '')
			or isnull(t.[high], '') <> isnull(s.[high], '')
			or isnull(t.[low], '') <> isnull(s.[low], '')
			or isnull(t.[close], '') <> isnull(s.[close], '')
			or isnull(t.volume, '') <> isnull(s.volume, '')))
		then
			update set
				t.[open] = s.[open],
				t.[high] = s.[high],
				t.[low] = s.[low],
				t.[close] = s.[close],
				t.volume = s.volume,
				t.updatedAt = sysdatetime()
		when not matched then
			insert (ticker, [date], [open], [high], [low], [close], volume, ingestedAt, updatedAt)
			values (s.ticker, s.[date], s.[open], s.[high], s.[low], s.[close], s.volume, sysdatetime(), sysdatetime());

		print 'Table ''clean.us_stocks'' is loaded and upserted.';

	end try
	begin catch
		print 'Error occurred in Staging Layer Load'
	end catch
end;