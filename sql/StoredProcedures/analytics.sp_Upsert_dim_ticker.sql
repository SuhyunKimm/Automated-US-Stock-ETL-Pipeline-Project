/*
================================================================================
Procedure: analytics.sp_Upsert_dim_ticker
Purpose  : Load and upsert cleaned ticker data from the clean layer into the
           analytics (gold) layer.
           - Upsert data into analytics.dim_ticker using MERGE
Author   : Suhyun Kim
Created  : 2026-01-26
Notes    :
  - This procedure upserts clean.dim_ticker into analytics.dim_ticker (gold layer).
================================================================================
*/

use USStocks;
go
-- Silver Layer : Staging

create or alter procedure analytics.sp_Upsert_dim_ticker
as
begin
	set nocount on;

	merge analytics.dim_ticker as t
	using clean.dim_ticker as s
	on t.ticker = s.ticker

	when matched then
		update set
			t.company_name = s.company_name,
			t.country = s.country,
			t.industry = s.industry,
			t.market = s.market,
			t.currency = s.currency,
			t.updated_at = sysdatetime()
	when not matched then
		insert (ticker, company_name, country, industry, market, currency, updated_at)
		values (
			s.ticker,
			s.company_name,
			s.country,
			s.industry,
			s.market,
			s.currency,
			sysdatetime()
		);
	
	print 'Table ''analytics.dim_ticker'' is updated.';
end;