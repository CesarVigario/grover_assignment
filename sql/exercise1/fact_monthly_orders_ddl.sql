create table grover_dwh.fact_monthly_orders (
	dim_month_id INT not null,
	dim_order_status_id INT,
	dim_country_id INT,
	dim_category_id INT,
	number_of_orders INT,
	total_order_value NUMERIC
)