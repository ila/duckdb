# ivm-extension

Incrementally maintain view `result` using query `PRAGMA ivm('materialized_view_name')` or `PRAGMA ivm_options('catalog_name', 'schema_name', 'materialized_view_name')`.

Read more [here](https://github.com/cwida/ivm-extension/blob/ivm-optimizer-rule/VLDB%20Summer%20School%202023%20Poster.pdf)

## Usage
Given a base table `my_table` and a view `my_view` on top of `my_table`, this extension incrementally computes the changes to the view `my_result` when the underlying table `my_table` changes. 

This extension replicates changes to base table `my_table` in the delta table `delta_my_table`. This table additionally has a multiplicity column `_duckdb_ivm_multiplicity` of type `BOOL`, and a timestamp column to track refresh operations on the views. `duckdb_ivm_multiplicity=true` means insertions and `_duckdb_ivm_multiplicity` means deletion. 
Updates to a row in base table `my_table` are modelled as deletion + insertion.

Note: if the extension is called with an in-memory database, the tables do not get created. We envision this use case for cross-system incremental view maintenance.
If the extension is called with a persistent database, the tables are created and the changes are stored in the delta tables.
In order to attach a database, use `duckdb my_database.db' from the CLI.

Here is an example. First create the base table and the view:
```SQL
CREATE TABLE sales (order_id INT, product_name VARCHAR(1), amount INT, date_ordered DATE);
```
Insert sample data:
```SQL
INSERT INTO sales VALUES (1, 'a', 100, '2023-01-10'), (2, 'b', 50, '2023-01-12'), (3, 'a', 75, '2023-01-15'), (4, 'c', 60, '2023-01-18'), (5, 'b', 30, '2023-01-20'), (6, 'b', 35, '2023-01-21');
```
Now create a materialized view with ONE of the following queries:
```SQL
CREATE MATERIALIZED VIEW product_sales AS SELECT product_name, SUM(amount) AS total_amount, COUNT(*) AS total_orders FROM sales WHERE product_name = 'a' OR product_name = 'b' GROUP BY product_name;
```

```SQL
CREATE MATERIALIZED VIEW product_sales_1 AS SELECT * FROM sales WHERE product_name = 'a';
```
```SQL
CREATE MATERIALIZED VIEW product_sales AS SELECT SUM(amount) AS total_amount FROM sales;
```
Look at the content:
```SQL
SELECT * FROM product_sales; -- to check the view content
```
Now we can modify the base table. Insertion example:
```SQL
INSERT INTO sales VALUES (7, 'a', 90, '2023-01-21'), (8, 'b', 10, '2023-01-25'), (9, 'a', 20, '2023-01-26'), (10, 'c', 45, '2023-01-28');
```
Deletion example:
```SQL
DELETE FROM sales WHERE order_id = 1;
```
Update example:
```SQL
UPDATE sales SET amount = 200 WHERE order_id = 3;
```

### Incrementally maintaining view *result*
There are two ways to trigger the refresh of a materialized view:
```SQL
PRAGMA ivm('product_sales'); -- will use current catalog and schema
```
```SQL
PRAGMA ivm_options('test_sales', 'main', 'product_sales'); -- specifying catalog and schema
```
The output of the above will be the table `delta_product_sales`, which will contain incremental processing of the changes to the base table of view `product_sales`. 

### Extent of SQL Support
* Only SELECT, FILTER, GROUP BY, PROJECTION
* Aggregations supported: SUM, COUNT
* Joins, nested-subqueries and other SQL clauses like HAVING **not supported**.

### Known issues
IVM on queries in which the base table returns no data because of a `WHERE` clause, **will fail**. So, while using `WHERE`, always ensure that the base table returns a non-empty result. More details in [this issue](https://github.com/cwida/ivm-extension/issues/10).

## Running Tests
* The tests are present in `project_root/tests`. 
* Create a folder `duckdb_project_root/test/ivm`.
* Copy the test files into the above folder.
* Use the `unittest` executable and provide name of the test as program argument.

