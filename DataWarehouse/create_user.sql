-- Active: 1737947105157@@172.18.0.2@5432@postgres@public
CREATE USER biruni WITH PASSWORD 'P@ssw0rd';
ALTER ROLE biruni WITH LOGIN CREATEDB CREATEROLE;
CREATE DATABASE biruni;

-- now you can login with biruni
