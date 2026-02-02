/*
================================================================================
Procedure: clean.sp_Load_dim_ticker
Purpose  : 
	Load and upsert cleaned ticker data from the python script into the staging
	layer.	
Author   : Suhyun Kim
Created  : 2026-02-01
Notes    :
================================================================================
*/

use USStocks;
go

create or alter procedure clean.sp_Load_dim_ticker
	@ticker nvarchar(10),
	@companyName nvarchar(100),
	@country nvarchar(50),
	@industry nvarchar(50),
	@market nvarchar(50),
	@currency nvarchar(50)
as
begin
	set nocount on;

	merge clean.dim_ticker as t
	using (
		select
			ticker = @ticker,
			companyName = @companyName,
			country = @country,
			industry = @industry,
			market = @market,
			currency = @currency
		) as s
	on t.ticker = s.ticker

	when matched then
		update set
			t.companyName = s.companyName,
			t.country = s.country,
			t.industry = s.industry,
			t.market = s.market,
			t.currency = s.currency,
			t.ingestedAt = sysdatetime()
	when not matched then
		insert (ticker, companyName, country, industry, market, currency, ingestedAt)
		values (
			s.ticker,
			s.companyName,
			s.country,
			s.industry,
			s.market,
			s.currency,
			sysdatetime()
		);
	
	print 'Table ''clean.dim_ticker'' is upated and inserted.';
end;