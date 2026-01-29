--SQL Server DDL (Create Tables)
--First Select the Sales_Analytics DB context and then run the following statements
--If already exists, do this for a clean start
/*drop table src.OrderLines;
drop table src.Orders;
drop table src.Products;
drop table src.Customers;
drop table src.Stores;

drop schema src;
*/

create schema src;


-- Customers Table
CREATE TABLE src.Customers (
    customer_id INT PRIMARY KEY,
    name NVARCHAR(100),
    email NVARCHAR(100),
    region NVARCHAR(50)
);

-- Products Table
CREATE TABLE src.Products (
    product_id INT PRIMARY KEY,
    name NVARCHAR(100),
    category NVARCHAR(50),
    price DECIMAL(10,2)
);

-- Stores Table
CREATE TABLE src.Stores (
    store_id INT PRIMARY KEY,
    location NVARCHAR(100),
    manager NVARCHAR(100)
);

-- Orders Table
CREATE TABLE src.Orders (
    order_id INT PRIMARY KEY,
    customer_id INT FOREIGN KEY REFERENCES src.Customers(customer_id),
    store_id INT FOREIGN KEY REFERENCES src.Stores(store_id),
    order_date DATE,
    status NVARCHAR(50)
);

-- OrderLines Table
CREATE TABLE src.OrderLines (
    orderline_id INT PRIMARY KEY,
    order_id INT FOREIGN KEY REFERENCES src.Orders(order_id),
    product_id INT FOREIGN KEY REFERENCES src.Products(product_id),
    quantity INT,
    unit_price DECIMAL(10,2),
    discount DECIMAL(4,2)
);
--------------------------------------------------------------------------
--Bulk Insert Tables from CSV files( Modify the file structure according to the system )
-- Load Customers
--when generated locally in windows
BULK INSERT src.Customers
FROM 'D:\Demo\Sales_Analytics\opt\airflow\data\Source\Customers.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);

--when generated via docker(linux)
BULK INSERT src.Customers
FROM 'D:\Demo\Sales_Analytics\opt\airflow\data\Source\Customers.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0A',
    TABLOCK
);

-- Load Products
--when generated locally in windows
BULK INSERT src.Products
FROM 'D:\Demo\Sales_Analytics\opt\airflow\data\Source\Products.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);
--when generated via docker(linux)
BULK INSERT src.Products
FROM 'D:\Demo\Sales_Analytics\opt\airflow\data\Source\Products.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0A',
    TABLOCK
);
-- Load Stores
--when generated locally in windows
BULK INSERT src.Stores
FROM 'D:\Demo\Sales_Analytics\opt\airflow\data\Source\Stores.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);
--when generated via docker(linux)
BULK INSERT src.Stores
FROM 'D:\Demo\Sales_Analytics\opt\airflow\data\Source\Stores.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0A',
    TABLOCK
);

-- Load Orders
--when generated locally in windows
BULK INSERT src.Orders
FROM 'D:\Demo\Sales_Analytics\opt\airflow\data\Source\Orders.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);
--when generated via docker(linux)
BULK INSERT src.Orders
FROM 'D:\Demo\Sales_Analytics\opt\airflow\data\Source\Orders.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0A',
    TABLOCK
);

-- Load OrderLines
--when generated locally in windows
BULK INSERT src.OrderLines
FROM 'D:\Demo\Sales_Analytics\opt\airflow\data\Source\OrderLines.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);
--when generated via docker(linux)
BULK INSERT src.OrderLines
FROM 'D:\Demo\Sales_Analytics\opt\airflow\data\Source\OrderLines.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0A',
    TABLOCK
);

--------------------------------------------------------------------------
--To check if data is loaded correctly in the source tables
select * from src.Customers;
select count(distinct(customer_id)) from src.Customers;-- should be 1000
select * from src.Products;
select count(distinct(product_id)) from src.Products;-- should be 1000
select * from src.Stores;
select count(distinct(store_id))from src.Stores;-- should be 1000
select * from src.Orders;
select count(distinct(order_id)) from src.Orders;-- should be 1000
select * from src.OrderLines;
select count(distinct(orderline_id)) from src.OrderLines;-- should be 1000


