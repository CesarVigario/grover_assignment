insert into grover_dwh.fact_monthly_orders
select 
	m.dim_month_id, 
	os.dim_order_status_id,
	c.dim_country_id,
	ca.dim_category_id, 
	coalesce(count(distinct o.order_id),0) as number_of_orders,
	coalesce(round(sum(o.order_value)::numeric, 2),0) as total_order_value
from grover_dwh.dim_orders o
join grover_dwh.dim_month m on m.dim_month_id = TO_CHAR(o.creation_date, 'YYYYMM')::INT
left join grover_dwh.dim_order_status os on os.status_name = o.order_status
left join grover_dwh.dim_country c on c.country = o.country
left join grover_dwh.dim_category ca on ca.category = o.category
group by dim_month_id, dim_order_status_id, dim_country_id, dim_category_id
order by dim_month_id, dim_order_status_id, dim_country_id, dim_category_id;