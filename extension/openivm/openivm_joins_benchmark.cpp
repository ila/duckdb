//
// Created by sppub on 24/04/25.
//

#include "../compiler/include/compiler_extension.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "include/openivm_benchmark.hpp"

#include <cmath>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <random>

namespace duckdb {


/// Create the join table, and return the dir it's in.
string CreateJoinTable(const int start_records, const int delta_records, const int pool, bool is_right) {
	string data_dir = "/tmp/data/";
	// Add pool size to dir as well as it can change the output.
	string scale_dir = data_dir + "t" + DoubleToString(start_records) + "p" + DoubleToString(pool) + "/";

	string l_or_r = is_right ? "right" : "left";

	auto joins_path = scale_dir + l_or_r + "_joins.tbl";
	auto joins_path_new = scale_dir + l_or_r + "_joins_new_" + to_string(delta_records) + ".tbl";

	LocalFileSystem fs;

	if (!fs.DirectoryExists(data_dir)) {
		fs.CreateDirectory(data_dir);
	}

	if (!fs.DirectoryExists(scale_dir)) {
		fs.CreateDirectory(scale_dir);
	}

	// Make there be a bit of overlap in terms of values by bounding the random values to the tuple/insert count.
	if (!fs.FileExists(joins_path)) {
		std::ofstream outfile(joins_path);

		// Start at i=0 for better modulo logic
		for (int i = 0; i < start_records; ++i) {
			// Format: `i,'left_start',tid` (tid based on pool)
			// Or:    `i,'right_start',tid` (tid based on pool)
			outfile << i << "," << l_or_r << "_start," << i % pool << '\n';
		}
	}

	if (!fs.FileExists(joins_path_new)) {
		std::ofstream outfile(joins_path_new);

		for (int i = 0; i < delta_records; ++i) {
			// Format: `i,'left_add',tid` or `i,'right_add',tid`.
			outfile << start_records + i << "," << l_or_r << "_add," << i % pool << '\n';
		}
		// Is this for guaranteed mismatches or something? Probably not needed here.
		/*
		for (int i = 0; i < 100; ++i) {
			outfile << "0,Join_xxxxx," << GetRandomValue(insertions) << "\n";
		}
        */
	}
	return scale_dir;
}

/* Eventually, it would be good to have one value per insert/update/delete (no L/R distinction).
 * Then, automatically balance in this way:
 * - 50/50 (1:1)
 * - 60/40 (3:2)
 * - 75/25 (3:1)
 * - 80/20 (4:1)
 * - 90/10 (9:1)
 */

bool first_time = true;
bool ivm_joins = true;

// TODO: change input variables to: start_l, start_r, inserts_l, inserts_r, tid_pool (l=left, r=right)
void RunIVMJoinsBenchmark(int start_left, int start_right, int add_left, int add_right, int tid_pool) {
	// Usage: call ivm_benchmark_joins(10, 1000, 1000);

	string db_name = "joins.db";
	string view_name = "simple_join";
	string left_table_name = "left_side";
	string right_table_name = "right_side";

	// The following part is largely copied from openivm_groups_benchmark, with some changes for joins.
	// create the data folder
	LocalFileSystem fs;
	fs.CreateDirectory("data");

	auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
	std::cout << std::put_time(std::localtime(&now), "%c ") << "Benchmark initialized..."
	<< "\n";

	std::cout << std::put_time(std::localtime(&now), "%c ") << "Generating data..."
	<< "\n";

	// Generating tables. `scale_dir`s end with a '/'.
	string left_scale_dir = CreateJoinTable(start_left, add_left, tid_pool, false);
	string right_scale_dir = CreateJoinTable(start_right, add_right, tid_pool, true);
	std::cout << std::put_time(std::localtime(&now), "%c ") << "Generated joins data..."
	<< "\n";

	DuckDB db(db_name);
	Connection con(db);
	con.Query("set ivm_files_path = 'data';");
	now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
	std::cout << std::put_time(std::localtime(&now), "%c ") << "Connected to the database instance..." << '\n';

	std::chrono::milliseconds load_time = std::chrono::milliseconds(0);

	// Part 2
	if (first_time) {
		// Load the joins.
		auto start_time = std::chrono::high_resolution_clock::now();
		// Left side.
//		con.Query("CREATE TABLE " + left_table_name + "(increment_id INTEGER PRIMARY KEY, dummy_name VARCHAR, tid_left INTEGER);");
		con.Query("CREATE TABLE " + left_table_name + "(increment_id INTEGER, dummy_name VARCHAR, tid_left INTEGER);");
		std::cout << std::put_time(std::localtime(&now), "%c ") << "Loading data (left)..."
				  << "\n";

		string left_query = "COPY " + left_table_name + " FROM '" + left_scale_dir + "left_joins.tbl' (DELIMITER ',');";
		auto r_1 = con.Query(left_query);
		if (r_1->HasError()) {
			fs.RemoveFile(db_name);
			throw InternalException("Failed to load joins data (left): %s", r_1->GetError().c_str());
		}
		// Right side.
//		con.Query("CREATE TABLE " + right_table_name + "(geo_id INTEGER PRIMARY KEY, dummy_location VARCHAR, tid_right INTEGER);");
		con.Query("CREATE TABLE " + right_table_name + "(geo_id INTEGER, dummy_location VARCHAR, tid_right INTEGER);");
		std::cout << std::put_time(std::localtime(&now), "%c ") << "Loading data (right)..."
				  << "\n";

		auto r_2 = con.Query("COPY " + right_table_name + " FROM '" + right_scale_dir + "right_joins.tbl' (DELIMITER ',');");
		if (r_2->HasError()) {
			fs.RemoveFile(db_name);
			throw InternalException("Failed to load joins data (right): %s", r_2->GetError().c_str());
		}

		auto end_time = std::chrono::high_resolution_clock::now();
		load_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
		now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
		std::cout << std::put_time(std::localtime(&now), "%c ") << "Loaded join tables..."
				  << "\n";
	}
	// Count the records in the tables
	auto count_left = con.Query("SELECT COUNT(*) FROM " + left_table_name + ";")->GetValue(0, 0).ToString();
	std::cout << std::put_time(std::localtime(&now), "%c ")
			  << "Rows inserted in the left base table: " << Format(count_left) << "\n";

	auto count_right = con.Query("SELECT COUNT(*) FROM " + right_table_name + ";")->GetValue(0, 0).ToString();
	std::cout << std::put_time(std::localtime(&now), "%c ")
			  << "Rows inserted in the right base table: " << Format(count_right) << "\n";

	// Create a materialised view with a join.
	string base_join_query = (
		"SELECT * FROM " + left_table_name + " INNER JOIN " + right_table_name + " ON tid_left = tid_right;"
		// "SELECT * FROM " + left_table_name + ", " + right_table_name + " WHERE tid_left = tid_right;"
	);
	string materialized_view = "CREATE MATERIALIZED VIEW " + view_name + " AS " + base_join_query;
	std::chrono::milliseconds materialized_view_time = std::chrono::milliseconds(0);
	std::chrono::milliseconds index_time = std::chrono::milliseconds(0);
	if (first_time) {
		con.Query("set execute = false");

		auto r = con.Query(materialized_view);
		if (r->HasError()) {
			fs.RemoveFile(db_name);
			throw InternalException("Failed to create materialized view: %s", r->GetError().c_str());
		}
		// First one has static name. Second and third one use the view name.
		auto system_queries = duckdb::CompilerExtension::ReadFile("data/ivm_system_tables.sql");
		auto ddl_queries = duckdb::CompilerExtension::ReadFile("data/ivm_compiled_queries_simple_join.sql");
		auto index_queries = duckdb::CompilerExtension::ReadFile("data/ivm_index_simple_join.sql");

		auto start_time = std::chrono::high_resolution_clock::now();
		r = con.Query(system_queries);
		if (r->HasError()) {
			fs.RemoveFile(db_name);
			// FIXME: Code fails here due to duplicates:
			//  "Constraint Error: Duplicate key "view_name: simple_join, table_name: delta_right_side" violates primary key constraint."
			throw InternalException("Failed to create system tables: %s", r->GetError().c_str());
		}
		r = con.Query(ddl_queries);
		if (r->HasError()) {
			fs.RemoveFile(db_name);
			throw InternalException("Failed to create compiled queries: %s", r->GetError().c_str());
		}
		auto end_time = std::chrono::high_resolution_clock::now();
		materialized_view_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
		now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
		std::cout << std::put_time(std::localtime(&now), "%c ") << "Materialized view created..."
				  << "\n";

		std::cout << std::put_time(std::localtime(&now), "%c ") << "Query: " << materialized_view << "\n";

		start_time = std::chrono::high_resolution_clock::now();
		con.Query(index_queries);
		end_time = std::chrono::high_resolution_clock::now();
		index_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
		now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
		std::cout << std::put_time(std::localtime(&now), "%c ") << "ART index created..."
				  << "\n";

		first_time = false;  // TODO: Is this really needed?
	}
	auto count_matches = con.Query("SELECT COUNT(*) FROM " + view_name + ";")->GetValue(0, 0).ToString();
	std::cout << std::put_time(std::localtime(&now), "%c ")
			  << "Rows inserted in the materialized view: " << Format(count_matches) << "\n";

	// inserting new data in the delta table
	std::cout << std::put_time(std::localtime(&now), "%c ") << "Applying modifications to the base table..."
			  << "\n";
	std::chrono::milliseconds copy_time;
	{
		auto start_time = std::chrono::high_resolution_clock::now();
		if (add_left < 0) {
			auto r = con.Query("COPY " + left_table_name + " FROM '" + left_scale_dir + "left_joins_new_" +
			                   to_string(add_left) + ".tbl' (DELIMITER ',');");
			if (r->HasError()) {
				fs.RemoveFile(db_name);
				throw InternalException("Failed to load new left table data: %s", r->GetError().c_str());
			}
		}
		if (add_right > 0) {
			auto r = con.Query("COPY " + right_table_name + " FROM '" + right_scale_dir + "right_joins_new_" +
			              to_string(add_right) + ".tbl' (DELIMITER ',');");
			if (r->HasError()) {
				fs.RemoveFile(db_name);
				throw InternalException("Failed to load new right table data: %s", r->GetError().c_str());
			}
		}
		// Skipping updates/deletions...
		auto end_time = std::chrono::high_resolution_clock::now();
		copy_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
	}
	now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
	std::cout << std::put_time(std::localtime(&now), "%c ") << "New data inserted..."
			  << "\n";

	auto r = con.Query("SELECT COUNT(*) FROM delta_" + view_name + ";");
	if (r->HasError()) {
		fs.RemoveFile(db_name);
		throw InternalException("Failed to count delta simple_join: %s", r->GetError().c_str());
	}
	auto count_delta_groups = r->GetValue(0, 0).ToString();
	std::cout << std::put_time(std::localtime(&now), "%c ")
			  << "Rows inserted in delta_simple_join: " << Format(count_delta_groups) << "\n";


	// we want to see each query time individually -> we save the result to a file and then parse it
	std::chrono::milliseconds compile_time;
	{
		auto start_time = std::chrono::high_resolution_clock::now();
		if (ivm_joins) {
			r = con.Query("PRAGMA ivm('simple_join');");
			if (r->HasError()) {
				fs.RemoveFile(db_name);
				throw InternalException("Failed to run the query: %s", r->GetError().c_str());
			}
			ivm_joins = false;
		}
		auto end_time = std::chrono::high_resolution_clock::now();
		compile_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
	}
	now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
	std::cout << std::put_time(std::localtime(&now), "%c ") << "Queries compiled..."
			  << "\n";


	auto queries = ReadQueries("ivm_upsert_queries_" + view_name + ".sql");
	const string& insert_query = queries[0];
	const string& update_timestamp_query = queries[1];
	const string& delete_mul_false = queries[2];
	const string& insert_mul_true = queries[3];
	const string& delete_query_2_join = queries[4];
	const string& delete_query_3_sides = queries[5]; // TODO: Split into two queries.

	// now we insert
	std::chrono::milliseconds insert_time;
	{
		auto start_time = std::chrono::high_resolution_clock::now();
		r = con.Query(insert_query);
		if (r->HasError()) {
			fs.RemoveFile(db_name);
			throw InternalException("Failed to insert data: %s", r->GetError().c_str());
		}
		auto end_time = std::chrono::high_resolution_clock::now();
		insert_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
	}
	now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
	std::cout << std::put_time(std::localtime(&now), "%c ") << "IVM insertion performed..."
	          << "\n";

	auto count_delta_simple_joins = con.Query("SELECT COUNT(*) FROM delta_" + view_name + ";")->GetValue(0, 0).ToString();
	// to make sure the cache is warm
	std::cout << std::put_time(std::localtime(&now), "%c ")
	          << "Rows inserted in delta_" << view_name << ": " << Format(count_delta_simple_joins) << "\n";

	// updating metadata
	con.Query(update_timestamp_query);

	// performing the upsert
	std::chrono::milliseconds maintain_time;
	{
		auto start_time = std::chrono::high_resolution_clock::now();
		// Causes issues with the `exists` clause (correlated subquery).
		con.Query(delete_mul_false);
		con.Query(insert_mul_true);
		auto end_time = std::chrono::high_resolution_clock::now();
		maintain_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
		now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
		std::cout << std::put_time(std::localtime(&now), "%c ") << "IVM changes performed..."
				  << "\n";
	}
	std::chrono::milliseconds delete_time;
	{
		auto start_time = std::chrono::high_resolution_clock::now();
		con.Query(delete_query_2_join);
		con.Query(delete_query_3_sides);
		auto end_time = std::chrono::high_resolution_clock::now();
		delete_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
		now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
		std::cout << std::put_time(std::localtime(&now), "%c ") << "IVM deletion performed..."
				  << "\n";
	}
	auto new_count_simple_join = con.Query("SELECT COUNT(*) FROM " + view_name + ";")->GetValue(0, 0).ToString();
	std::cout << std::put_time(std::localtime(&now), "%c ")
			  << "Rows in the materialized view after the changes: " << Format(new_count_simple_join) << "\n";


	// finally, we measure the time to run the query without IVM
	auto count_left_new = con.Query("SELECT COUNT(*) FROM " + left_table_name + " ;")->GetValue(0, 0).ToString();
	std::cout << std::put_time(std::localtime(&now), "%c ")
	          << "Total rows in the left base table: " << Format(count_left_new) << "\n";

	auto count_right_new = con.Query("SELECT COUNT(*) FROM " + right_table_name + " ;")->GetValue(0, 0).ToString();
	std::cout << std::put_time(std::localtime(&now), "%c ")
			  << "Total rows in the right base table: " << Format(count_right_new) << "\n";

	std::chrono::milliseconds query_groups_time;
	{
		auto start_time = std::chrono::high_resolution_clock::now();
		con.Query(base_join_query);
		auto end_time = std::chrono::high_resolution_clock::now();
		query_groups_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
		now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
		std::cout << std::put_time(std::localtime(&now), "%c ") << "Query executed..." << "\n";
	}

	auto total_time =
	    materialized_view_time + index_time + compile_time + insert_time + maintain_time + delete_time + (copy_time / 2);


	// printing the benchmark results
	std::cout << "\nBenchmark Results:" << '\n';
	std::cout << std::setw(56) << std::setfill('-') << "" << '\n';
	std::cout << std::left << std::setw(43) << "| Operation"
			  << "| Time (ms) |" << '\n';
	std::cout << std::setw(56) << std::setfill('-') << "" << '\n';
	std::cout << "| " << std::left << std::setw(41) << "Data load"
			  << "| " << std::setw(9) << load_time.count() << " |" << '\n';
	std::cout << "| " << std::left << std::setw(41) << "Materialized view (+ tables) creation"
			  << "| " << std::setw(9) << materialized_view_time.count() << " |" << '\n';
	std::cout << "| " << std::left << std::setw(41) << "ART index creation"
			  << "| " << std::setw(9) << index_time.count() << " |" << '\n';
	std::cout << "| " << std::left << std::setw(41) << "Copy"
			  << "| " << std::setw(9) << copy_time.count() << " |" << '\n';
	std::cout << "| " << std::left << std::setw(41) << "Queries compilation"
			  << "| " << std::setw(9) << compile_time.count() << " |" << '\n';
	std::cout << "| " << std::left << std::setw(41) << "IVM insertion"
			  << "| " << std::setw(9) << insert_time.count() << " |" << '\n';
	std::cout << "| " << std::left << std::setw(41) << "IVM changes"
			  << "| " << std::setw(9) << maintain_time.count() << " |" << '\n';
	std::cout << "| " << std::left << std::setw(41) << "IVM deletion"
			  << "| " << std::setw(9) << delete_time.count() << " |" << '\n';
	std::cout << "| " << std::left << std::setw(41) << "Total IVM time"
			  << "| " << std::setw(9) << total_time.count() << " |" << '\n';
	std::cout << "| " << std::left << std::setw(41) << "Query without IVM"
			  << "| " << std::setw(9) << query_groups_time.count() << " |" << '\n';
	std::cout << std::setw(56) << std::setfill('-') << "" << '\n';
}

} // namespace.