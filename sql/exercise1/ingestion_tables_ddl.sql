-- Create Order table 
CREATE TABLE IF NOT EXISTS grover.orders (
    order_id CHAR(44) PRIMARY KEY,
    creation_date DATE NOT NULL,
    order_value FLOAT,
    country_id INT,
    status_code INT, 
    category_id INT
);

-- Create country table 
CREATE TABLE IF NOT EXISTS grover.country (
    id INT PRIMARY KEY,
    country VARCHAR(56) NOT NULL
);

-- Create order_status table 
CREATE TABLE IF NOT EXISTS grover.order_status (
    status_code INT PRIMARY KEY,
    status VARCHAR(9) NOT NULL CHECK (status in ('APPROVED', 'DECLINED', 'CANCELLED'))
);

-- Create category table 
CREATE TABLE IF NOT EXISTS grover.category (
    id INT PRIMARY KEY,
    category VARCHAR(25) NOT NULL
);
