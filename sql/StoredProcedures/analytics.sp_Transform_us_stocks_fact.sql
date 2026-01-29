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
				open_price = s.[Open],
				high_price = s.[High],
				low_price = s.[Low],
				close_price = s.[Close],
				volume = s.Volume
			from 
				clean.us_stocks s
			inner join 
				analytics.dim_date d
			on s.[Date] = d.Fulldate
			inner join 
				analytics.dim_ticker t
			on s.Ticker = t.ticker
		), rtn as (
			select *,
				daily_return =
					case 
						when lag(close_price) over (partition by tickerId order by dateKey) is null
						then null
					else
					(close_price - lag(close_price) over (partition by tickerId order by dateKey))
					/ lag(close_price) over (partition by tickerId order by dateKey)
					end,
				cumulative_return =
					(close_price / first_value(close_price) over (partition by tickerId order by dateKey)) - 1
			from base
		), vol as (
			select *,
				volatility_20d = 
					stdev(daily_return) over (partition by tickerId order by dateKey rows between 19 preceding and current row)
			from rtn
		)
		merge into analytics.fact_stock_daily As t
		using (
			select
				dateKey, tickerId, open_price, high_price, low_price, close_price, volume, daily_return, cumulative_return, volatility_20d
			from vol 
		) as s
		on (t.dateKey = s.dateKey and t.tickerId = s.tickerId)
		when matched then
			update set
				t.open_price = s.open_price,
				t.high_price = s.high_price,
				t.low_price = s.low_price,
				t.close_price = s.close_price,
				t.volume = s.volume,
				t.daily_return = s.daily_return,
				t.cumulative_return = s.cumulative_return,
				t.volatility_20d = s.volatility_20d,
				t.updated_at = sysdatetime()
		when not matched by target then
			insert (dateKey, tickerId, open_price, high_price, low_price, close_price, volume, daily_return, cumulative_return, volatility_20d, updated_at)
			values (s.dateKey, s.tickerId, s.open_price, s.high_price, s.low_price, s.close_price, s.volume, s.daily_return, s.cumulative_return, s.volatility_20d, sysdatetime());

		print 'Table ''analytics.us_stocks'' is transformed and upserted.';

	end try
	begin catch
		print 'Error occurred in Transforming Layer Load'
	end catch
end