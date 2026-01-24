/* NEED TO CHANGE
================================================================================
Procedure: sp_Transform_Gold_Layer
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

-- Gold Layer : Transformation
create procedure sp_Load_Transformation_Layer
as
begin
	set nocount on;

