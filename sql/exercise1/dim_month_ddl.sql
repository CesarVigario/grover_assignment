create table if not exists grover_dwh.dim_month (
	dim_month_id INT primary key,
	month_name CHAR(9) not null,
	start_of_month DATE not null,
	end_of_month DATE not null
)