-- Active: 1740743903688@@127.0.0.1@3306@OLTP_Database
-- 1. Retrieve all transactions with customer and product details:
SELECT 
    t.TransactionID,
    c.CustomerName,
    p.ProductName,
    p.CategoryID,
    td.Quantity,
    td.Price,
    t.TotalPrice,
    t.TransactionDate
FROM 
    Transaction t
JOIN 
    Customer c ON t.CustomerID = c.CustomerID
JOIN 
    TransactionDetail td ON t.TransactionID = td.TransactionID
JOIN 
    Product p ON td.ProductID = p.ProductID;


SELECT p.`ProductID`, p.`ProductName`, pc.`CategoryName`, sc.`SubcategoryName`, p.`Price`
FROM `Product` p  
JOIN `ProductCategory` pc  ON p.`CategoryID` = pc.`CategoryID`
JOIN `ProductSubcategory` sc ON p.`SubcategoryID` = sc.`SubcategoryID`;


-- 2. Retrieve all products in the "Electronics" category:
SELECT 
    p.ProductName,
    p.Price,
    pc.CategoryName,
    ps.SubcategoryName
FROM 
    Product p
JOIN 
    ProductCategory pc ON p.CategoryID = pc.CategoryID
JOIN 
    ProductSubcategory ps ON p.SubcategoryID = ps.SubcategoryID
WHERE 
    pc.CategoryName = 'Electronics';

-- 3. Retrieve all promotions applied to transactions:
SELECT 
    t.TransactionID,
    p.PromotionName,
    p.StartDate,
    p.EndDate
FROM 
    Transaction t
JOIN 
    Promotion p ON t.PromotionID = p.PromotionID;