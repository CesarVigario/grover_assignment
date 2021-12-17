CREATE TABLE IF NOT EXISTS grover_dwh.dim_category (
	dim_category_id SERIAL primary key,
    category_id INT NOT NULL,
    category VARCHAR(25) NOT null,
    effectiveDate DATE not null,
    expirationDate DATE
);