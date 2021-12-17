CREATE TABLE IF NOT EXISTS grover_dwh.dim_date (
    date_dim_id INT PRIMARY KEY,
    date_actual DATE NOT NULL,
    month_name CHAR(9) NOT NULL
)