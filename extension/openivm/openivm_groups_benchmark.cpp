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

bool create_groups = true;
bool ivm_groups = true;

void RunIVMGroupsBenchmark(int tuples, int insertions, int updates, int deletes) {

	// usage: call ivm_benchmark('groups', 1, 10, 10, 10);

	// create the data folder
	LocalFileSystem fs;
	fs.CreateDirectory("data");

	auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
	std::cout << std::put_time(std::localtime(&now), "%c ") << "Benchmark initialized..."
	          << "\n";

	std::cout << std::put_time(std::localtime(&now), "%c ") << "Generating data..."
	          << "\n";

	// generating groups
	CreateTable(tuples, insertions);
	std::cout << std::put_time(std::localtime(&now), "%c ") << "Generated groups data..."
	          << "\n";

	DuckDB db("groups.db");
	Connection con(db);
	con.Query("set ivm_files_path = 'data';");
	now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
	std::cout << std::put_time(std::localtime(&now), "%c ") << "Connected to the database instance..." << '\n';

	std::chrono::milliseconds load_time = std::chrono::milliseconds(0);

	if (create_groups) {
		// loading the groups
		auto start_time = std::chrono::high_resolution_clock::now();
		con.Query("CREATE TABLE groups(group_id INTEGER, group_index VARCHAR, group_value INTEGER);");
		std::cout << std::put_time(std::localtime(&now), "%c ") << "Loading data..."
		          << "\n";

		auto r = con.Query("COPY groups FROM '/tmp/data/t" + to_string(tuples) + "/groups.tbl' (DELIMITER ',');");
		if (r->HasError()) {
			fs.RemoveFile("groups.db");
			throw InternalException("Failed to load groups data: %s", r->GetError().c_str());
		}
		auto end_time = std::chrono::high_resolution_clock::now();
		load_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
		now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
		std::cout << std::put_time(std::localtime(&now), "%c ") << "Loaded groups..."
		          << "\n";
	}

	auto count_groups = con.Query("SELECT COUNT(*) FROM groups;")->GetValue(0, 0).ToString();
	std::cout << std::put_time(std::localtime(&now), "%c ")
	          << "Rows inserted in the base table: " << Format(count_groups) << "\n";

	string query_groups = "SELECT group_index, SUM(group_value) AS total_value FROM groups GROUP BY group_index;";
	string materialized_view = "CREATE MATERIALIZED VIEW query_groups AS " + query_groups;

	std::chrono::milliseconds materialized_view_time = std::chrono::milliseconds(0);
	std::chrono::milliseconds index_time = std::chrono::milliseconds(0);

	if (create_groups) {

		con.Query("set execute = false");

		auto r = con.Query(materialized_view);
		if (r->HasError()) {
			fs.RemoveFile("groups.db");
			throw InternalException("Failed to create materialized view: %s", r->GetError().c_str());
		}

		auto system_queries = duckdb::CompilerExtension::ReadFile("data/ivm_system_tables_client.sql");
		auto ddl_queries = duckdb::CompilerExtension::ReadFile("data/ivm_compiled_queries_query_groups.sql");
		auto index_queries = duckdb::CompilerExtension::ReadFile("data/ivm_index_query_groups.sql");

		auto start_time = std::chrono::high_resolution_clock::now();
		r = con.Query(system_queries);
		if (r->HasError()) {
			fs.RemoveFile("groups.db");
			throw InternalException("Failed to create system tables: %s", r->GetError().c_str());
		}
		r = con.Query(ddl_queries);
		if (r->HasError()) {
			fs.RemoveFile("groups.db");
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

		create_groups = false;
	}

	auto count_query_groups = con.Query("SELECT COUNT(*) FROM query_groups;")->GetValue(0, 0).ToString();
	std::cout << std::put_time(std::localtime(&now), "%c ")
	          << "Rows inserted in the materialized view: " << Format(count_query_groups) << "\n";

	// inserting new data in the delta table
	std::cout << std::put_time(std::localtime(&now), "%c ") << "Applying modifications to the base table..."
	          << "\n";
	auto start_time = std::chrono::high_resolution_clock::now();
	auto r = con.Query("COPY groups FROM '/tmp/data/t" + to_string(tuples) + "/groups_new_" + to_string(insertions) +
	                   ".tbl' (DELIMITER ',');");
	if (r->HasError()) {
		fs.RemoveFile("groups.db");
		throw InternalException("Failed to load new groups data: %s", r->GetError().c_str());
	}
	// updating
	if (updates > 0) {
		r = con.Query("UPDATE groups SET group_value = 100 WHERE group_id < " + to_string(updates) + ";");
		if (r->HasError()) {
			fs.RemoveFile("groups.db");
			throw InternalException("Failed to update groups data: %s", r->GetError().c_str());
		}
	}
	// deleting
	if (deletes > 0) {
		r = con.Query("DELETE FROM groups WHERE group_id > " + to_string(tuples + insertions - deletes) + ";");
		if (r->HasError()) {
			fs.RemoveFile("groups.db");
			throw InternalException("Failed to delete groups data: %s", r->GetError().c_str());
		}
	}
	auto end_time = std::chrono::high_resolution_clock::now();
	auto copy_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
	now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
	std::cout << std::put_time(std::localtime(&now), "%c ") << "New data inserted..."
	          << "\n";

	r = con.Query("SELECT COUNT(*) FROM delta_groups;");
	if (r->HasError()) {
		fs.RemoveFile("groups.db");
		throw InternalException("Failed to count delta groups: %s", r->GetError().c_str());
	}
	auto count_delta_groups = r->GetValue(0, 0).ToString();
	std::cout << std::put_time(std::localtime(&now), "%c ")
	          << "Rows inserted in delta_groups: " << Format(count_delta_groups) << "\n";

	// we want to see each query time individually -> we save the result to a file and then parse it

	start_time = std::chrono::high_resolution_clock::now();
	if (ivm_groups) {
		r = con.Query("PRAGMA ivm('query_groups');");
		if (r->HasError()) {
			fs.RemoveFile("groups.db");
			throw InternalException("Failed to run the query: %s", r->GetError().c_str());
		}
		ivm_groups = false;
	}
	end_time = std::chrono::high_resolution_clock::now();
	auto compile_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
	now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
	std::cout << std::put_time(std::localtime(&now), "%c ") << "Queries compiled..."
	          << "\n";

	auto queries = ReadQueries("ivm_upsert_queries_query_groups.sql");
	auto insert_query = queries[0];
	auto update_timestamp_query = queries[1];
	auto upsert_query = queries[2];
	auto delete_query_1 = queries[3];
	auto delete_query_2 = queries[4];
	auto delete_query_3 = queries[5];

	// now we insert
	start_time = std::chrono::high_resolution_clock::now();
	r = con.Query(insert_query);
	if (r->HasError()) {
		fs.RemoveFile("groups.db");
		throw InternalException("Failed to insert data: %s", r->GetError().c_str());
	}
	end_time = std::chrono::high_resolution_clock::now();
	auto insert_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
	now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
	std::cout << std::put_time(std::localtime(&now), "%c ") << "IVM insertion performed..."
	          << "\n";

	auto count_delta_query_groups = con.Query("SELECT COUNT(*) FROM delta_query_groups;")->GetValue(0, 0).ToString();
	// to make sure the cache is warm
	std::cout << std::put_time(std::localtime(&now), "%c ")
	          << "Rows inserted in delta_query_groups: " << Format(count_delta_query_groups) << "\n";

	// updating metadata
	con.Query(update_timestamp_query);

	// performing the upsert
	start_time = std::chrono::high_resolution_clock::now();
	con.Query(upsert_query);
	end_time = std::chrono::high_resolution_clock::now();
	auto upsert_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
	now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
	std::cout << std::put_time(std::localtime(&now), "%c ") << "IVM upsert performed..."
	          << "\n";

	start_time = std::chrono::high_resolution_clock::now();
	con.Query(delete_query_1);
	con.Query(delete_query_2);
	con.Query(delete_query_3);
	end_time = std::chrono::high_resolution_clock::now();
	auto delete_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
	now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
	std::cout << std::put_time(std::localtime(&now), "%c ") << "IVM deletion performed..."
	          << "\n";

	auto new_count_query_groups = con.Query("SELECT COUNT(*) FROM query_groups;")->GetValue(0, 0).ToString();
	std::cout << std::put_time(std::localtime(&now), "%c ")
	          << "Rows in the materialized view after the upsert: " << Format(new_count_query_groups) << "\n";

	// finally, we measure the time to run the query without IVM
	auto count_groups_new = con.Query("SELECT COUNT(*) FROM groups;")->GetValue(0, 0).ToString();
	std::cout << std::put_time(std::localtime(&now), "%c ")
	          << "Total rows in the groups table: " << Format(count_groups_new) << "\n";

	start_time = std::chrono::high_resolution_clock::now();
	con.Query(query_groups);
	end_time = std::chrono::high_resolution_clock::now();
	auto query_groups_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
	now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
	std::cout << std::put_time(std::localtime(&now), "%c ") << "Query executed..."
	          << "\n";

	auto total_time =
	    materialized_view_time + index_time + compile_time + insert_time + upsert_time + delete_time + (copy_time / 2);

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
	std::cout << "| " << std::left << std::setw(41) << "IVM upsert"
	          << "| " << std::setw(9) << upsert_time.count() << " |" << '\n';
	std::cout << "| " << std::left << std::setw(41) << "IVM deletion"
	          << "| " << std::setw(9) << delete_time.count() << " |" << '\n';
	std::cout << "| " << std::left << std::setw(41) << "Total IVM time"
	          << "| " << std::setw(9) << total_time.count() << " |" << '\n';
	std::cout << "| " << std::left << std::setw(41) << "Query without IVM"
	          << "| " << std::setw(9) << query_groups_time.count() << " |" << '\n';
	std::cout << std::setw(56) << std::setfill('-') << "" << '\n';
}

} // namespace duckdb
