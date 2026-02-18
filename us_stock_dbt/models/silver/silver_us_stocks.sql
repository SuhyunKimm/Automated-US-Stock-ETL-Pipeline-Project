-- us_stocks (silver layer)

{{ config (
	materialized='table'
)}}

select
    upper(trim(ticker)) as ticker,
    cast(date as date) as date,
	cast(open as float64) as open,
	cast(high as float64) as high,
	cast(low as float64) as low,
	cast(close as float64) as close,
	cast(volume as int64) as volume,
	ingestedAt as ingestedAt
from {{ source('bronze', 'bronze_us_stocks') }}
where 
    ticker is not null 
    and trim(ticker) != ''
    and date is not null