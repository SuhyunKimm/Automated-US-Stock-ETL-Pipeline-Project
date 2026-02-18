-- us_market_holidays (silver layer)

{{ config (
	materialized='table'
)}}

select
	cast(date as date) as date,
	status as status,
	datetime(concat(date, ' ', startTime)) as startTime,
	datetime(concat(date, ' ', endTime)) as endTime,
	description as description,
	ingestedAt as ingestedAt
from {{ source('bronze', 'bronze_us_market_holidays') }}
where date is not null