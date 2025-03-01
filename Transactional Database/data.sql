-- Active: 1740743903688@@127.0.0.1@3306@OLTP_Database
use OLTP_Database;

INSERT INTO Employee (EmployeeName, NationalID, JobTitle, Gender, HireDate, Salary) VALUES
('John Doe', 'NID001', 'Store Manager', 'Male', '2020-01-15', 60000.00),
('Jane Smith', 'NID002', 'Sales Associate', 'Female', '2021-03-20', 35000.00),
('Emily Johnson', 'NID003', 'Cashier', 'Female', '2022-05-01', 30000.00),
('Michael Brown', 'NID004', 'Stock Clerk', 'Male', '2022-08-30', 28000.00),
('Sarah Wilson', 'NID005', 'Sales Associate', 'Female', '2021-11-10', 35000.00),
('David Garcia', 'NID006', 'Store Manager', 'Male', '2019-06-01', 60000.00),
('Laura Martinez', 'NID007', 'Cashier', 'Female', '2022-12-20', 30000.00),
('Tony Scott', 'NID008', 'Stock Clerk', 'Male', '2023-01-15', 28000.00),
('Linda Lee', 'NID009', 'Sales Associate', 'Female', '2020-08-25', 35000.00),
('George Harris', 'NID010', 'Warehouse Supervisor', 'Male', '2018-04-12', 45000.00),
('Alice Walker', 'NID011', 'Assistant Manager', 'Female', '2021-07-01', 50000.00),
('Brian Williams', 'NID012', 'Sales Associate', 'Male', '2021-09-15', 35000.00),
('Michelle Kim', 'NID013', 'Store Manager', 'Female', '2020-10-12', 60000.00),
('Kevin Brown', 'NID014', 'Cashier', 'Male', '2022-03-30', 30000.00),
('Jessica White', 'NID015', 'Sales Associate', 'Female', '2021-11-05', 35000.00),
('Edward Green', 'NID016', 'Stock Clerk', 'Male', '2022-01-19', 28000.00),
('Hannah Baker', 'NID017', 'Sales Associate', 'Female', '2020-06-29', 35000.00),
('Noah Clark', 'NID018', 'Store Manager', 'Male', '2019-12-25', 60000.00),
('Olivia Martinez', 'NID019', 'Cashier', 'Female', '2023-02-03', 30000.00),
('James Anderson', 'NID020', 'Stock Clerk', 'Male', '2021-08-21', 28000.00),
('Ava Davis', 'NID021', 'Sales Associate', 'Female', '2022-11-30', 35000.00),
('William Johnson', 'NID022', 'Assistant Manager', 'Male', '2022-05-22', 50000.00),
('Sophia Thompson', 'NID023', 'Sales Associate', 'Female', '2020-01-10', 35000.00),
('Jacob Lewis', 'NID024', 'Cashier', 'Male', '2022-07-23', 30000.00),
('Mia Harris', 'NID025', 'Store Manager', 'Female', '2019-09-15', 60000.00),
('Zohie Maged', 'NID100', 'Stock Clerk', 'Female', '2023-10-01', 28000.00);
-- Table: Customer (100 entries)
INSERT INTO Customer (CustomerName, Phone, Gender, Age, City) VALUES
('Alice Wilson', '555-0001', 'Female', 34, 'New York'),
('David Lee', '555-0002', 'Male', 28, 'Los Angeles'),
('Sarah Gonzalez', '555-0003', 'Female', 45, 'Chicago'),
('Brian Kim', '555-0004', 'Male', 22, 'Houston'),
('Nina Patel', '555-0005', 'Female', 30, 'San Francisco'),
('Tom Nguyen', '555-0006', 'Male', 40, 'Miami'),
('Jessica Chen', '555-0007', 'Female', 29, 'Seattle'),
('Ryan Adams', '555-0008', 'Male', 33, 'Dallas'),
('Emma Robinson', '555-0009', 'Female', 26, 'Austin'),
('Liam Thompson', '555-0010', 'Male', 31, 'Boston'),
('Charlotte Hernandez', '555-0011', 'Female', 36, 'Phoenix'),
('Zoe Martin', '555-0012', 'Female', 27, 'Denver'),
('Logan Rodriguez', '555-0013', 'Male', 44, 'Atlanta'),
('Chloe Torres', '555-0014', 'Female', 38, 'Charlotte'),
('Oliver Lee', '555-0015', 'Male', 32, 'San Diego'),
('Abigail Lopez', '555-0016', 'Female', 29, 'Philadelphia'),
('Lucas Gonzalez', '555-0017', 'Male', 24, 'San Antonio'),
('Mason Wright', '555-0018', 'Male', 35, 'San Jose'),
('Sophia Hill', '555-0019', 'Female', 41, 'Indianapolis'),
('Ethan Campbell', '555-0020', 'Male', 29, 'Jacksonville'),
-- (Add similar lines to fill 100 customers)
('Customer 100', '555-0100', 'Male', 27, 'Washington');

-- Table: ProductCategory (10 entries)
INSERT INTO ProductCategory (CategoryName) VALUES
('Electronics'),
('Clothing'),
('Home Appliances'),
('Books'),
('Toys'),
('Furniture'),
('Beauty Products'),
('Sporting Goods'),
('Kitchen Supplies'),
('Gardening Equipment');

-- Table: ProductSubcategory (20 entries)
INSERT INTO ProductSubcategory (SubcategoryName, CategoryID) VALUES
('Smartphones', 1),
('Laptops', 1),
('Men\'s Apparel', 2),
('Women\'s Apparel', 2),
('Kitchen Appliances', 3),
('Home Decor', 6),
('Fiction', 4),
('Non-Fiction', 4),
('Board Games', 5),
('Outdoor Equipment', 8),
('Televisions', 1),
('Tablets', 1),
('Casual Wear', 2),
('Formal Wear', 2),
('Small Appliances', 3),
('Large Appliances', 3),
('Makeup', 7),
('Skincare', 7),
('Sports Equipment', 8),
('Toys for Kids', 5);

-- Table: Product (100 entries)
INSERT INTO Product (ProductName, CategoryID, SubcategoryID, Price, ProductionDate, ExpirationDate) VALUES
('Samsung Galaxy S21', 1, 1, 799.99, '2021-01-05', NULL),
('Dell XPS 13', 1, 2, 999.99, '2020-10-10', NULL),
('Levi\'s 501 Jeans', 2, 3, 59.99, '2021-07-01', NULL),
('Nike Women\'s Running Shoes', 2, 4, 89.99, '2022-06-15', NULL),
('Oster Blender', 3, 5, 49.99, '2020-09-20', NULL),
('Living Room Sofa', 6, 6, 499.99, '2021-12-01', NULL),
('The Great Gatsby', 4, 7, 15.99, '2020-04-10', '2024-04-10'),
('The Lean Startup', 4, 8, 20.99, '2020-01-10', '2025-01-10'),
('Uno Card Game', 5, 9, 19.99, '2019-10-01', NULL),
('Camping Tent', 8, 10, 120.00, '2021-05-01', NULL),
('Apple iPhone 13', 1, 1, 899.99, '2022-09-01', NULL),
('Sony 65" 4K TV', 1, 2, 1099.99, '2021-05-10', NULL),
('Nike Men\'s Sports T-Shirt', 2, 3, 29.99, '2020-11-15', NULL),
('Keurig Coffee Maker', 3, 5, 89.99, '2021-08-20', NULL),
('Dining Table Set', 6, 6, 799.99, '2022-04-01', NULL),
('Die Hard Blu-ray', 4, 7, 12.99, '2021-07-19', NULL),
('How to Win Friends and Influence People', 4, 8, 7.99, '2021-09-01', NULL),
('Razor Electric Scooter', 5, 9, 299.99, '2023-01-15', NULL),
('20-Piece Cookware Set', 3, 5, 149.99, '2022-06-25', NULL),
('Teddy Bear', 5, 10, 29.99, '2023-03-30', NULL),
('Yoga Mat', 8, 12, 29.99, '2022-11-07', NULL),
-- (Add similar lines to fill 100 products)
('Product 100', 1, 7, 89.99, '2023-08-01', NULL);

-- Table: Store (20 entries)
INSERT INTO Store (Location, Size, ManagerID) VALUES
('123 Main St, New York, NY', 'Large', 1),
('456 Elm St, Los Angeles, CA', 'Medium', 1),
('789 Maple St, Chicago, IL', 'Small', 2),
('101 South St, Miami, FL', 'Large', 6),
('202 North Ave, Seattle, WA', 'Medium', 5),
('303 West Ave, Austin, TX', 'Small', 4),
('404 East Rd, San Francisco, CA', 'Large', 3),
('505 Central Blvd, Dallas, TX', 'Medium', 7),
('606 Park St, Boston, MA', 'Small', 8),
('707 Broadway, Denver, CO', 'Medium', 1),
('808 Lake St, Minneapolis, MN', 'Small', 2),
('909 Hill Rd, Atlanta, GA', 'Large', 3),
('1010 Valley Ave, San Diego, CA', 'Medium', 4),
('1111 Ocean Ave, Miami, FL', 'Small', 5),
('1212 River Rd, Philadelphia, PA', 'Large', 6),
('1313 Pine St, Phoenix, AZ', 'Medium', 2),
('1414 Maple St, Portland, OR', 'Small', 8),
('1515 Oak St, Las Vegas, NV', 'Large', 1),
-- (Add similar lines to fill 20 stores)
('Store 20', 'Small', 2);

-- Table: Promotion (20 entries)
INSERT INTO Promotion (PromotionName, Type, StartDate, EndDate) VALUES
('Summer Sale', 'Discount', '2023-06-01', '2023-09-01'),
('Holiday Special', 'Bundle', '2022-12-01', '2023-12-31'),
('Back to School', 'Discount', '2023-08-01', '2023-09-15'),
('Spring Clearance', 'Clearance', '2023-03-01', '2023-04-30'),
('Weekend Flash Sale', 'Flash', '2023-07-15', '2023-07-16'),
('Black Friday', 'Discount', '2023-11-24', '2023-11-25'),
('Cyber Monday', 'Discount', '2023-11-27', '2023-11-27'),
('New Year Special', 'Bundle', '2023-12-31', '2024-01-01'),
('Valentine\'s Day Offer', 'Discount', '2023-02-01', '2023-02-14'),
('Easter Sale', 'Discount', '2023-04-01', '2023-04-15'),
('Labor Day Weekend', 'Discount', '2023-09-01', '2023-09-05'),
('Memorial Day Sale', 'Discount', '2023-05-25', '2023-05-31'),
('Anniversary Sale', 'Bundle', '2023-10-01', '2023-10-31'),
('Fall Clearance', 'Clearance', '2023-09-01', '2023-09-30'),
('Weekend Deal', 'Discount', '2023-07-05', '2023-07-06'),
('Clearance Event', 'Clearance', '2023-03-15', '2023-03-30'),
('Student Discount', 'Discount', '2023-07-01', '2023-08-31'),
('25% Off Electronics', 'Discount', '2023-11-01', '2023-11-30'),
('Winter Wonderland Sale', 'Bundle', '2023-12-01', '2023-12-31'),
('Flash Weekend Sale', 'Flash', '2023-08-15', '2023-08-16');

-- Table: Transaction (100 entries)
INSERT INTO Transaction (CustomerID, StoreID, EmployeeID, TransactionDate, PromotionID, TotalPrice) VALUES
(1, 1, 2, '2023-07-10 14:30:00', 1, 879.98),
(2, 2, 3, '2023-07-11 10:15:00', NULL, 89.99),
(3, 3, 4, '2023-07-12 11:00:00', 2, 35.98),
(4, 1, 1, '2023-07-13 16:45:00', NULL, 799.99),
(5, 1, 2, '2023-07-14 09:30:00', NULL, 1109.98),
(6, 2, 3, '2023-07-15 12:00:00', 1, 59.99),
(7, 3, 4, '2023-07-16 14:00:00', NULL, 20.00),
(8, 1, 2, '2023-07-17 10:00:00', NULL, 29.99),
(9, 2, 3, '2023-07-18 11:15:00', 2, 799.99),
(10, 3, 4, '2023-07-19 15:00:00', NULL, 899.99),
(11, 1, 2, '2023-08-01 10:00:00', NULL, 1399.99),
(12, 2, 3, '2023-08-02 14:15:00', 1, 99.99),
(13, 3, 4, '2023-08-03 11:30:00', NULL, 54.99),
(14, 1, 1, '2023-08-04 13:45:00', 2, 749.99),
(15, 2, 3, '2023-08-05 09:00:00', NULL, 49.99),
(16, 3, 4, '2023-08-06 18:30:00', 1, 14.99),
(17, 1, 2, '2023-08-07 10:30:00', NULL, 24.99),
(18, 2, 3, '2023-08-08 16:00:00', 2, 399.99),
(19, 3, 4, '2023-08-09 12:15:00', NULL, 12.99),
(20, 1, 1, '2023-08-10 11:45:00', NULL, 199.99);

SELECT * from `Transaction`;
-- (Continue similar lines to fill 100 transactions)
-- (100, 3, 4, '2023-08-24 11:15:00', NULL, 29.99);

-- Table: TransactionDetail (100 entries)
INSERT INTO TransactionDetail (TransactionID, ProductID, Quantity, Price) VALUES
(1, 1, 1, 799.99),
(1, 2, 1, 999.99),
(1, 3, 1, 59.99),
(2, 4, 1, 89.99),
(3, 7, 2, 31.98),
(4, 1, 1, 799.99),
(5, 1, 1, 899.99),
(5, 3, 1, 59.99),
(6, 4, 1, 89.99),
(7, 5, 1, 49.99),
(8, 6, 1, 499.99),
(9, 1, 1, 799.99),
(10, 7, 1, 15.99),
(11, 8, 1, 20.99),
(12, 9, 1, 19.99),
(13, 10, 1, 120.99),
(14, 11, 1, 899.99),
(15, 12, 1, 1099.99),
(16, 13, 1, 29.99),
(17, 14, 1, 89.99),
(18, 15, 1, 799.99),
(19, 16, 1, 12.99);
-- (Add similar lines to fill 100 transaction details)
-- (100, 7, 1, 29.99);

INSERT INTO `TransactionDetail` (TransactionID, ProductID, Quantity, Price) VALUES (1, 2, 1, 999.99);
SELECT * FROM TransactionDetail;

COMMIT;