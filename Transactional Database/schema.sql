-- Active: 1740743903688@@127.0.0.1@3306@TEST
-- Create User
-- Create the user

SELECT CURRENT_USER();

-- Create Database
CREATE DATABASE OLTP_Database;
USE OLTP_Database;

-- Table: Employee
CREATE TABLE Employee (
    EmployeeID INT PRIMARY KEY AUTO_INCREMENT,
    EmployeeName VARCHAR(100) NOT NULL,
    NationalID VARCHAR(20) NOT NULL,
    JobTitle VARCHAR(50) NOT NULL,
    Gender VARCHAR(10) NOT NULL,
    HireDate DATE NOT NULL,
    Salary FLOAT NOT NULL
);

-- Table: Customer
CREATE TABLE Customer (
    CustomerID INT PRIMARY KEY AUTO_INCREMENT,
    CustomerName VARCHAR(100) NOT NULL,
    Phone VARCHAR(15) NOT NULL,
    Gender VARCHAR(10) NOT NULL,
    Age INT NOT NULL,
    City VARCHAR(50) NOT NULL
);

-- Table: ProductCategory
CREATE TABLE ProductCategory (
    CategoryID INT PRIMARY KEY AUTO_INCREMENT,
    CategoryName VARCHAR(100) NOT NULL
);

-- Table: ProductSubcategory
CREATE TABLE ProductSubcategory (
    SubcategoryID INT PRIMARY KEY AUTO_INCREMENT,
    SubcategoryName VARCHAR(100) NOT NULL,
    CategoryID INT NOT NULL,
    FOREIGN KEY (CategoryID) REFERENCES ProductCategory(CategoryID)
);

-- Table: Product
CREATE TABLE Product (
    ProductID INT PRIMARY KEY AUTO_INCREMENT,
    ProductName VARCHAR(100) NOT NULL,
    CategoryID INT NOT NULL,
    SubcategoryID INT NOT NULL,
    Price DECIMAL(10, 2) NOT NULL,
    ProductionDate DATE NOT NULL,
    ExpirationDate DATE,
    FOREIGN KEY (CategoryID) REFERENCES ProductCategory(CategoryID),
    FOREIGN KEY (SubcategoryID) REFERENCES ProductSubcategory(SubcategoryID)
);

-- Table: Store
CREATE TABLE Store (
    StoreID INT PRIMARY KEY AUTO_INCREMENT,
    Location VARCHAR(200) NOT NULL,
    Size VARCHAR(50) NOT NULL,
    ManagerID INT NOT NULL,
    FOREIGN KEY (ManagerID) REFERENCES Employee(EmployeeID)
);

-- Table: Promotion
CREATE TABLE Promotion (
    PromotionID INT PRIMARY KEY AUTO_INCREMENT,
    PromotionName VARCHAR(100) NOT NULL,
    Type VARCHAR(50) NOT NULL,
    StartDate DATE NOT NULL,
    EndDate DATE NOT NULL
);

-- Table: Transaction

CREATE TABLE Transaction (
    TransactionID INT PRIMARY KEY AUTO_INCREMENT,
    CustomerID INT NOT NULL,
    StoreID INT NOT NULL,
    EmployeeID INT NOT NULL,
    TransactionDate DATETIME NOT NULL,
    PromotionID INT,
    TotalPrice DECIMAL(10, 2) NOT NULL,
    FOREIGN KEY (CustomerID) REFERENCES Customer(CustomerID),
    FOREIGN KEY (StoreID) REFERENCES Store(StoreID),
    FOREIGN KEY (EmployeeID) REFERENCES Employee(EmployeeID),
    FOREIGN KEY (PromotionID) REFERENCES Promotion(PromotionID)
);


-- Table: TransactionDetail
CREATE TABLE TransactionDetail (
    TransactionDetailID INT PRIMARY KEY AUTO_INCREMENT,
    TransactionID INT NOT NULL,
    ProductID INT NOT NULL,
    Quantity INT NOT NULL,
    Price DECIMAL(10, 2) NOT NULL,
    FOREIGN KEY (TransactionID) REFERENCES OLTP_Database.Transaction(TransactionID),
    FOREIGN KEY (ProductID) REFERENCES Product(ProductID)
);
