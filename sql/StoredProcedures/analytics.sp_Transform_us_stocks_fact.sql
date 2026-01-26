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
create procedure sp_Load_Transformation_Layer
as
begin
	set nocount on;

