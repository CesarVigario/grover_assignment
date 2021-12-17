create table grover_dwh.dim_country (
	dim_country_id SERIAL PRIMARY key,
	country_id INT NOT NULL,
    country VARCHAR(56) NOT null,
    effectiveDate DATE not null,
    expirationDate DATE
);