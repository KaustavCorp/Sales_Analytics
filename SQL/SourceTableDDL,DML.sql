--SQL Server DDL (Create Tables)

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
BULK INSERT src.Customers
FROM 'D:\Demo\Sales_Analytics\Data\Source\Customers.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);

-- Load Products
BULK INSERT src.Products
FROM 'D:\Demo\Sales_Analytics\Data\Source\Products.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);

-- Load Stores
BULK INSERT src.Stores
FROM 'D:\Demo\Sales_Analytics\Data\Source\Stores.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);

-- Load Orders
BULK INSERT src.Orders
FROM 'D:\Demo\Sales_Analytics\Data\Source\Orders.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);

-- Load OrderLines
BULK INSERT src.OrderLines
FROM 'D:\Demo\Sales_Analytics\Data\Source\OrderLines.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    TABLOCK
);

--------------------------------------------------------------------------
--To check if data is loaded correctly in the source tables
select * from src.Customers;
select * from src.Products;
select * from src.Stores;
select * from src.Orders;
select * from src.OrderLines;


