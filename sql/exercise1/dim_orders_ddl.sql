create table if not exists grover_dwh.dim_orders (
	dim_orders_id SERIAL primary key,
	order_id CHAR(44)  not null,
	creation_date DATE not null,
	country VARCHAR(56), 
	order_status VARCHAR(9) not null,
	category VARCHAR(10),
	order_value FLOAT
)