/*
================================================================================
Procedure: analytics.sp_Transform_us_stocks_fact
Purpose  : Transform cleaned stock data from clean.us_stocks into a fact table
           for analytics and reporting.
Author   : Suhyun Kim
Created  : 2026-01-26
Notes    :
  - Implements the gold layer of the Medallion architecture.
  - Produces the final fact table used as a source for Power BI reports.
================================================================================
*/
-- Gold Layer : Transformation

use USStocks;
go

create or alter procedure analytics.sp_Transform_us_stocks_fact
as
begin
	set nocount on;

	begin try
		with base as (
			select
				dateKey = d.dateKey,
				tickerId = t.tickerId,
				openPrice = s.[open],
				highPrice = s.[high],
				lowPrice = s.[low],
				closePrice = s.[close],
				volume = s.volume
			from 
				clean.us_stocks s
			inner join 
				analytics.dim_date d
			on s.[date] = d.fullDate
			inner join 
				analytics.dim_ticker t
			on s.ticker = t.ticker
		), rtn as (
			select *,
				dailyReturn =
					case 
						when lag(closePrice) over (partition by tickerId order by dateKey) is null
						then null
					else
					(closePrice - lag(closePrice) over (partition by tickerId order by dateKey))
					/ lag(closePrice) over (partition by tickerId order by dateKey)
					end,
				cumulativeReturn =
					(closePrice / first_value(closePrice) over (partition by tickerId order by dateKey)) - 1
			from base
		), vol as (
			select *,
				volatility20d = 
					stdev(dailyReturn) over (partition by tickerId order by dateKey rows between 19 preceding and current row)
			from rtn
		)
		merge into analytics.fact_stock_daily As t
		using (
			select
				dateKey, tickerId, openPrice, highPrice, lowPrice, closePrice, volume, dailyReturn, cumulativeReturn, volatility20d
			from vol 
		) as s
		on (t.dateKey = s.dateKey and t.tickerId = s.tickerId)
		when matched and 
			((t.openPrice <> s.openPrice)
			or (t.highPrice <> s.highPrice)
			or (t.lowPrice <> s.lowPrice)
			or (t.closePrice <> s.closePrice)
			or (t.volume <> s.volume))
		then
			update set
				t.openPrice = s.openPrice,
				t.highPrice = s.highPrice,
				t.lowPrice = s.lowPrice,
				t.closePrice = s.closePrice,
				t.volume = s.volume,
				t.dailyReturn = s.dailyReturn,
				t.cumulativeReturn = s.cumulativeReturn,
				t.volatility20d = s.volatility20d,
				t.updatedAt = sysdatetime()
		when not matched then
			insert (dateKey, tickerId, openPrice, highPrice, lowPrice, closePrice, volume, dailyReturn, cumulativeReturn, volatility20d, ingestedAt, updatedAt)
			values (s.dateKey, s.tickerId, s.openPrice, s.highPrice, s.lowPrice, s.closePrice, s.volume, s.dailyReturn, s.cumulativeReturn, s.volatility20d, sysdatetime(), sysdatetime());

		print 'Table ''analytics.us_stocks'' is transformed and upserted.';

	end try
	begin catch
		throw;
	end catch
end