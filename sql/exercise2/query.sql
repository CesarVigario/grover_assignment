-- function that calculates the number of days between two dates
create or replace function compute_interval_days(date, date) returns integer
	as 'select $1 - $2;'
	language sql
	immutable
	returns null on null input;

-- 2) SQL code
select 
	order_id, 
	order_status, 
	country, 
	string_agg(category, '|') over (partition by order_id),
	creation_date,
	compute_interval_days(current_date, date(creation_date)),
	order_value as order_amount,
	dense_rank() over (partition by country order by order_value desc) as country_order_value_index
from grover_dwh.dim_orders;

-- confirming there are only 3 orders with country_order_value_index = 1
select *
from
(
	select distinct
		order_id, 
		order_status, 
		country, 
		string_agg(category, '|') over (partition by order_id),
		creation_date,
		compute_interval_days(current_date, date(creation_date)),
		order_value as order_amount,
		dense_rank() over (partition by country order by order_value desc) as country_order_value_index
	from grover_dwh.dim_orders
) a 
where country_order_value_index = 1