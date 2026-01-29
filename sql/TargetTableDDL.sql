--If tables exist already
/*
drop table dw.FactSales;
drop table dw.DimDate;
drop table dw.DimProduct;
drop table dw.DimCustomer;
drop table dw.DimStore;
*/

Target DW SQL DDL
sql
-- sql/target_dw_ddl.sql
-- Create DW schema (if not exists)
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'dw')
    EXEC('CREATE SCHEMA dw');

-- Dimensions
CREATE TABLE dw.DimCustomer (
    customer_id INT PRIMARY KEY,
    customer_name NVARCHAR(100),
    customer_email NVARCHAR(100),
    region NVARCHAR(50)
);

CREATE TABLE dw.DimProduct (
    product_id INT PRIMARY KEY,
    product_name NVARCHAR(100),
    category NVARCHAR(50),
    list_price DECIMAL(12,2)
);

CREATE TABLE dw.DimStore (
    store_id INT PRIMARY KEY,
    store_location NVARCHAR(100),
    store_manager NVARCHAR(100)
);

CREATE TABLE dw.DimDate (
    date_id INT PRIMARY KEY,           -- yyyymmdd
    date DATE,
    year INT,
    month INT,
    day INT,
    month_name NVARCHAR(20),
    weekday_name NVARCHAR(20),
    quarter INT
);

-- Fact
CREATE TABLE dw.FactSales (
    orderline_id INT PRIMARY KEY,
    order_id INT,
    customer_id INT,
    store_id INT,
    product_id INT,
    date_id INT,
    order_date DATE,
    order_status NVARCHAR(20),
    quantity INT,
    unit_price DECIMAL(12,2),
    discount DECIMAL(4,2),
    gross_amount DECIMAL(18,2),
    net_amount DECIMAL(18,2),
    FOREIGN KEY (customer_id) REFERENCES dw.DimCustomer(customer_id),
    FOREIGN KEY (product_id)  REFERENCES dw.DimProduct(product_id),
    FOREIGN KEY (store_id)    REFERENCES dw.DimStore(store_id),
    FOREIGN KEY (date_id)     REFERENCES dw.DimDate(date_id)
);
