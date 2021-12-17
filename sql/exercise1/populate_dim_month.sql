INSERT INTO grover_dwh.dim_month
select 
	TO_CHAR(d, 'YYYYMM')::INT AS dim_month_id,
	to_char(d, 'YYYY"M"MM') as month_name, 
	d::date as start_of_month,
	(d + '1 month'::interval - '1 day'::interval )::date as end_of_month
from generate_series('2015-01-01'::date, '2040-12-31'::date, '1 month'::interval) d
;