-- Fact Table: Orders
CREATE TABLE IF NOT EXISTS fact_orders (
    OrderID VARCHAR(50),
    CustomerID VARCHAR(50),
    ProductID VARCHAR(50),
    PromoID VARCHAR(50),
    DateID INT,
    TotalAmount DECIMAL(10,2),
    DiscountApplied DECIMAL(10,2),
    PaymentMethod VARCHAR(50),
    PaymentStatus VARCHAR(50),
    DeliveryDate DATE,
    OrderStatus VARCHAR(50)
);

-- Fact Table: Inventory
CREATE TABLE IF NOT EXISTS fact_inventory (
    InventoryID VARCHAR(50),
    ProductID VARCHAR(50),
    SupplierID VARCHAR(50),
    StockLevel INT,
    LastUpdated TIMESTAMP
);

-- Dim Table: Customers
CREATE TABLE IF NOT EXISTS dim_customers (
    CustomerID VARCHAR(50) PRIMARY KEY,
    Name VARCHAR(255),
    Email VARCHAR(255),
    Address VARCHAR(255),
    SignupDate DATE
);

-- Dim Table: Products
CREATE TABLE IF NOT EXISTS dim_products (
    ProductID VARCHAR(50) PRIMARY KEY,
    Name VARCHAR(255),
    Category VARCHAR(100),
    Price DECIMAL(10,2),
    SupplierID VARCHAR(50),
);

-- Dim Table: Suppliers
CREATE TABLE IF NOT EXISTS dim_suppliers (
    SupplierID VARCHAR(50) PRIMARY KEY,
    Name VARCHAR(255),
    ContactInfo VARCHAR(255)
);

-- Dim Table: Promotions
CREATE TABLE IF NOT EXISTS dim_promotions (
    PromoID VARCHAR(50) PRIMARY KEY,
    Discount DECIMAL(5,2),
    StartDate DATE,
    EndDate DATE,
    CampaignType VARCHAR(100)
);

-- Dim Table: Time
CREATE TABLE IF NOT EXISTS dim_time (
    DateID INT PRIMARY KEY,
    Date DATE,
    Week INT,
    Month INT,
    Quarter INT,
    Year INT
);
