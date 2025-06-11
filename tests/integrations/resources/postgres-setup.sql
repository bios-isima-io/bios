DROP DATABASE IF EXISTS products;
CREATE DATABASE products;
\c products;

CREATE ROLE postgresuser with password 'P057gres%P&ssword!' REPLICATION LOGIN;
CREATE TABLE product (
  product_id varchar(40) CONSTRAINT firstkey UNIQUE PRIMARY KEY,
  product_name varchar(40),
  price integer
);
CREATE PUBLICATION dbz_publication for ALL TABLES;
