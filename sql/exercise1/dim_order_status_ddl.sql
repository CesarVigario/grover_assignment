CREATE TABLE IF NOT EXISTS grover_dwh.dim_order_status (
	dim_order_status_id SERIAL primary key,
    status_id INT not NULL,
    status_name VARCHAR(9) NOT null,
    effectiveDate DATE not null,
    expirationDate DATE
);