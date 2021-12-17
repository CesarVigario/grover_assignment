insert into grover_dwh.dim_orders (order_id, creation_date, country, order_status, category, order_value)
select distinct
	o.order_id, 
	creation_date,
	c.country,
	os.status,
	ca.category,
	o.order_value
from grover.orders o 
join grover.order_status os on os.status_code = o.status_code
left join grover.country c on c.id = o.country_id
left join grover.category ca on ca.id = o.category_id
order by order_id;