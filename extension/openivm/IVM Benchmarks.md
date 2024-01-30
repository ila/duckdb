### IVM Benchmarks

The following benchmarks are available for the IVM algorithm:
* TPC-H Query 1 (Modified)
* Groups:
  * `CREATE TABLE groups(group_index VARCHAR, group_value INTEGER);`
  * `SELECT group_index, SUM(group_value) FROM groups GROUP BY group_index;`
  * `group_index` is `"Group_"` + a random integer modulo `num_rows * 3`
  * `group_value` is a random integer between 1 and 100
  * updates are a variable percentage of the rows in 3 groups

Pitfalls:
* Copying the data

* Creating the index
  * When creating the materialized view, an ART index is added to the table with the group keys as keys
  * This operation adds overhead
  * Example: `call ivm_benchmark('groups', 2, 1, 10, 10);`
  * 153 million rows in the table (51 million groups) + 51 million new insertions (1.5 million groups)
  * Materialized view creation: 15174 ms (other operations negligible)
  * Query without IVM: 14016 ms



Strengths:
