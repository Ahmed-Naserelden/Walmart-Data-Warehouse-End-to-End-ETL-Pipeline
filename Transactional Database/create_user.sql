-- Active: 1740743903688@@127.0.0.1@3306@OLTP_Database
CREATE USER 'biruni'@'%' IDENTIFIED BY 'P@ssw0rd';
GRANT ALL PRIVILEGES ON *.* TO 'biruni'@'%' WITH GRANT OPTION;
--     StoreName VARCHAR(100) NOT NULL,
--     Phone VARCHAR(15) NOT NULL,      
--     City VARCHAR(50) NOT NULL
-- );
--

