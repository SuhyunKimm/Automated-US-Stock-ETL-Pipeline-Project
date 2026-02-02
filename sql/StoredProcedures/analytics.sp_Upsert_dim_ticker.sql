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

create or alter procedure analytics.sp_Upsert_dim_ticker
as
begin
	set nocount on;

	merge analytics.dim_ticker as t
	using clean.dim_ticker as s
	on t.ticker = s.ticker

	when matched and
		((isnull(t.companyName, '') <> isnull(s.companyName, ''))
		or (isnull(t.country, '') <> isnull(s.country, ''))
		or (isnull(t.industry, '') <> isnull(s.industry, ''))
		or (isnull(t.market, '') <> isnull(s.market, ''))
		or (isnull(t.currency, '') <> isnull(s.currency, '')))
	then
		update set
			t.companyName = s.companyName,
			t.country = s.country,
			t.industry = s.industry,
			t.market = s.market,
			t.currency = s.currency,
			t.updatedAt = sysdatetime()
	when not matched then
		insert (ticker, companyName, country, industry, market, currency, updatedAt)
		values (
			s.ticker,
			s.companyName,
			s.country,
			s.industry,
			s.market,
			s.currency,
			sysdatetime()
		);
	
	print 'Table ''analytics.dim_ticker'' is updated.';
end;