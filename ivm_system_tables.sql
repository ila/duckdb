create table if not exists _duckdb_ivm_views (view_name varchar primary key, sql_string varchar, type tinyint, plan varchar);

insert or replace into _duckdb_ivm_views values ('product_sales', 'select product_name, sum(amount) as total_amount, count(*) as total_orders from sales where product_name = ''a'' or product_name = ''b'' group by product_name', 0, 'abc');

