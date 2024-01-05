#include "../include/ivm/ivm_compile_upsert.hpp"

namespace duckdb {

string CompileAggregateGroups(string &view_name, optional_ptr<CatalogEntry> index_delta_view_catalog_entry,
                              vector<string> column_names) {
	// let's construct the query step by step
	auto index_catalog_entry = dynamic_cast<IndexCatalogEntry *>(index_delta_view_catalog_entry.get());
	auto key_ids = dynamic_cast<ART *>(index_catalog_entry)->column_ids;

	vector<string> keys;
	vector<string> aggregates;
	for (auto i = 0; i < key_ids.size(); i++) {
		keys.emplace_back(column_names[key_ids[i]]);
	}

	// let's find all the columns that are not keys
	// create an unordered_set from the column names for efficient lookups
	unordered_set<std::string> keys_set(keys.begin(), keys.end());

	for (auto &column : column_names) {
		if (keys_set.find(column) == keys_set.end() && column != "_duckdb_ivm_multiplicity") {
			// we discard the multiplicity column
			aggregates.push_back(column);
		}
	}

	/*
	 example output query (using the example from the README):
	 insert or replace into product_sales
	 with ivm_cte AS (
	 select product_name, sum(case when _duckdb_ivm_multiplicity = false then -total_amount else total_amount end)
	 as total_amount, sum(case when _duckdb_ivm_multiplicity = false then -total_orders else total_orders end) as
	 total_orders from delta_product_sales group by product_name) select product_sales.product_name,
	 sum(product_sales.total_amount + delta_product_sales.total_amount),
	 sum(product_sales.total_orders + delta_product_sales.total_orders)
	 from ivm_cte as delta_product_sales
	 left join product_sales
	 on product_sales.product_name = delta_product_name
	 group by product_sales.product_name;
	 */

	// this should be the end of the painful part (famous last words)

	// we start building the CTE (assuming it's always named ivm_cte)
	string cte_string = "with ivm_cte AS (\n";
	string cte_select_string = "select ";
	for (auto &key : keys) {
		cte_select_string = cte_select_string + key + ", ";
	}
	// now we sum the columns
	for (auto &column : aggregates) {
		cte_select_string = cte_select_string + "\n\tsum(case when _duckdb_ivm_multiplicity = false then -" + column +
		                    " else " + column + " end) as " + column + ", ";
	}
	// remove the last comma
	cte_select_string.erase(cte_select_string.size() - 2, 2);
	cte_select_string += "\n";
	// from
	string cte_from_string = "from delta_" + view_name + "\n";
	// group by
	string cte_group_by_string = "group by ";
	for (auto &key : keys) {
		cte_group_by_string = cte_group_by_string + key + ", ";
	}
	// remove the last comma
	cte_group_by_string.erase(cte_group_by_string.size() - 2, 2);
	// cte_group_by_string += "\n";

	cte_string = cte_string + cte_select_string + cte_from_string + cte_group_by_string + ")\n";

	// now build the external query
	// select is easy; both tables have the same columns
	// we assume that the delta view has the same columns as the view + the multiplicity column
	string select_string = "select ";
	// we only add the keys once
	for (auto &key : keys) {
		select_string = select_string + view_name + "." + key + ", ";
	}
	// now we sum the columns
	for (auto &column : aggregates) {
		select_string =
		    select_string + "\n\tsum(" + view_name + "." + column + " + delta_" + view_name + "." + column + "), ";
	}
	// remove the last comma
	select_string.erase(select_string.size() - 2, 2);
	select_string += "\n";

	// from is also easy, there's two tables
	// string from_string = "from " + view_name + ", delta_" + view_name + "\n";
	// string from_string = "from " + view_name + ", ivm_cte as delta_" + view_name + "\n";
	string from_string = "from ivm_cte as delta_" + view_name + "\n";
	// we need to left join the tables
	string join_string = "left join " + view_name + " on ";
	for (auto &key : keys) {
		join_string = join_string + view_name + "." + key + " = delta_" + view_name + "." + key + " and ";
	}
	// remove the last "and"
	join_string.erase(join_string.size() - 5, 5);
	join_string += "\n";

	// group by is also easy, we just group by the keys
	string group_by_string = "group by ";
	for (auto &key : keys) {
		group_by_string = group_by_string + view_name + "." + key + ", ";
	}
	// remove the last comma
	group_by_string.erase(group_by_string.size() - 2, 2);
	// group_by_string += "\n";

	string external_query_string = select_string + from_string + join_string + group_by_string + ";";
	string query_string = cte_string + external_query_string;
	string upsert_query = "insert or replace into " + view_name + "\n" + query_string + "\n";

	// we need to delete the rows with SUM or COUNT equal to 0
	string delete_query = "\ndelete from " + view_name + " where ";
	for (auto &column : aggregates) {
		delete_query += column + " = 0 and ";
	}
	// remove the last "and"
	delete_query.erase(delete_query.size() - 5, 5);
	delete_query += ";\n";
	upsert_query += delete_query;

	return upsert_query;
}

string CompileSimpleAggregates(string &view_name, const vector<string>& column_names) {
	// this is the case of SELECT COUNT(*) / SUM(column) without aggregation columns
	// we need to rewrite the query as a sum/difference of the multiplicity column
	/*
	UPDATE product_sales
	SET total_amount = total_amount - (
	SELECT amount
	FROM delta_product_sales
	WHERE _duckdb_ivm_multiplicity = false
	) + (
	SELECT amount
	FROM delta_product_sales
	WHERE _duckdb_ivm_multiplicity = true);
	*/
	string update_query = "update " + view_name + "\nset ";
	// there should be only one column here
	for (auto &column : column_names) {
		if (column != "_duckdb_ivm_multiplicity") { // we don't need the multiplicity column
			update_query += column + " = \n\t" + column + " \n\t\t- (select " + column + " from delta_" + view_name +
			                " where _duckdb_ivm_multiplicity = false)\n\t\t+ (select " + column + " from delta_" +
			                view_name + " where _duckdb_ivm_multiplicity = true);\n";
		}
	}
	// in this case, we choose not to delete from the main view if the SUM or COUNT is 0
	// in SQL, this query would still return a row with NULL or 0
	// todo update the row such that the sum is set to NULL if it is zero

	return update_query;
}

string CompileProjectionsFilters(string &view_name, const vector<string>& column_names) {
	// todo test with multiple insertions and deletions of the same row (updates)
	// we handle filters by performing a union
	// rewrite the query as union of the true multiplicity and difference of the false ones
	// we start with the difference
	string delete_query = "delete from " + view_name + " where exists (select 1 from delta_" + view_name + " where ";
	// we can't just do SELECT * since the multiplicity column does not exist
	string select_columns;
	// we need to add the keys
	for (auto &column : column_names) {
		if (column != "_duckdb_ivm_multiplicity") { // we don't need the multiplicity column
			delete_query += view_name + "." + column + " = delta_" + view_name + "." + column + " and ";
			select_columns += column + ", ";
		}
	}
	// set multiplicity to false
	delete_query += "_duckdb_ivm_multiplicity = false);\n";
	// erase the last comma and space from the column list
	select_columns.erase(select_columns.size() - 2, 2);

	// now we insert as well
	string insert_query = "insert into " + view_name + " select " + select_columns + " from delta_" + view_name +
	                      " where _duckdb_ivm_multiplicity = true;\n";
	return delete_query + insert_query;
}

} // namespace duckdb
