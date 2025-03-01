-- Active: 1737947105157@@172.18.0.2@5432@walmart_dwh@public

CREATE DATABASE Walmart_dwh;

CREATE TABLE dimEmployee (
    EmployeeSK SERIAL PRIMARY KEY, -- Surrogate Key (auto-increment in PostgreSQL)
    EmployeeBK INT,                -- Business Key (EmployeeID from OLTP)
    EmployeeName VARCHAR(100),
    NationalID VARCHAR(20),
    JobTitle VARCHAR(50),
    Gender VARCHAR(10),
    salary FLOAT,
    HireDate DATE
);

CREATE TABLE dimCustomer (
    CustomerSK SERIAL PRIMARY KEY, -- Surrogate Key
    CustomerBK INT,                -- Business Key (CustomerID from OLTP)
    CustomerName VARCHAR(100),
    Phone VARCHAR(15),
    Gender VARCHAR(10),
    Age INT,
    City VARCHAR(50)
);

CREATE TABLE dimProduct (
    ProductSK SERIAL PRIMARY KEY, -- Surrogate Key
    ProductBK INT,                -- Business Key (ProductID from OLTP)
    ProductName VARCHAR(100),
    CategoryName VARCHAR(100),
    SubcategoryName VARCHAR(100),
    Price DECIMAL(10, 2),
    ProductionDate DATE,
    ExpirationDate DATE
);


CREATE TABLE dimStore (
    StoreSK SERIAL PRIMARY KEY, -- Surrogate Key
    StoreBK INT,                -- Business Key (StoreID from OLTP)
    Location VARCHAR(200),
    Size VARCHAR(50),
    ManagerID INT   -- Denormalized for easier analysis
);

CREATE TABLE dimPromotion (
    PromotionSK SERIAL PRIMARY KEY, -- Surrogate Key
    PromotionBK INT,                -- Business Key (PromotionID from OLTP)
    PromotionName VARCHAR(100),
    Type VARCHAR(50),
    StartDate DATE,
    EndDate DATE
);

CREATE TABLE dimDate (
    DateSK SERIAL PRIMARY KEY, -- Surrogate Key
    Date DATE,                 -- Date in YYYY-MM-DD format
    Year INT,                  -- Year
    Quarter INT,               -- Quarter (1-4)
    Month INT,                 -- Month of the year
    Day INT,                   -- Day of the month
    DayOfWeek VARCHAR(10),     -- Day of the week (e.g., Monday)
    IsWeekend BOOLEAN,         -- Is the day a weekend? (TRUE/FALSE)
    IsHoliday BOOLEAN          -- Is the day a holiday? (TRUE/FALSE)
);

-- CREATE TABLE factTransaction (
CREATE TABLE factsales (
    factsaleSK SERIAL PRIMARY KEY, -- Surrogate Key
    TransactionID INT,
    CustomerSK INT,                   -- Foreign Key to dimCustomer
    EmployeeSK INT,                   -- Foreign Key to dimEmployee
    ProductSK INT,                    -- Foreign Key to dimProduct
    StoreSK INT,                      -- Foreign Key to dimStore
    PromotionSK INT,                  -- Foreign Key to dimPromotion
    DateSK INT,                       -- Foreign Key to dimTime
    Quantity INT,
    TotalPrice DECIMAL(10, 2),

    FOREIGN KEY (CustomerSK) REFERENCES dimCustomer(CustomerSK),
    FOREIGN KEY (EmployeeSK) REFERENCES dimEmployee(EmployeeSK),
    FOREIGN KEY (ProductSK) REFERENCES dimProduct(ProductSK),
    FOREIGN KEY (StoreSK) REFERENCES dimStore(StoreSK),
    FOREIGN KEY (PromotionSK) REFERENCES dimPromotion(PromotionSK) on delete set null,
    FOREIGN KEY (DateSK) REFERENCES dimDate(DateSK)


);

-- SELECT * FROM facttransaction;
SELECT * FROM factsales;
