create table product_sales as select product_name, sum(amount) as total_amount, count(*) as total_orders from sales where product_name = 'a' or product_name = 'b' group by product_name;

create table if not exists delta_sales as select *, true as _duckdb_ivm_multiplicity from sales limit 0;

create or replace view _duckdb_internal_product_sales_ivm as select product_name, sum(amount) as total_amount, count(*) as total_orders from sales where product_name = 'a' or product_name = 'b' group by product_name;

create table if not exists delta_product_sales as select *, true as _duckdb_ivm_multiplicity from product_sales limit 0;

-- code to propagate operations to the base table goes here
-- assuming the changes to be in the delta tables

