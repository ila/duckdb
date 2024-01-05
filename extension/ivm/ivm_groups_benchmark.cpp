#include "include/ivm_benchmark.hpp"

#include "../compiler/include/compiler_extension.hpp"
#include "duckdb/common/local_file_system.hpp"

#include <cmath>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <random>

namespace duckdb {

void RunIVMGroupsBenchmark(double scale_factor, int percentage_insertions, int percentage_updates, int percentage_deletes) {

	// usage: call ivm_benchmark('groups', 1, 10, 10, 10);

	/*
	 *
	 * insert or replace into query_groups
		with ivm_cte AS (
		select group_index,
				sum(case when _duckdb_ivm_multiplicity = false then -sum(group_value) else sum(group_value) end) as sum(group_value)
		from delta_query_groups
		group by group_index)
		select query_groups.group_index,
				sum(query_groups.sum(group_value) + delta_query_groups.sum(group_value))
		from ivm_cte as delta_query_groups
		left join query_groups on query_groups.group_index = delta_query_groups.group_index
		group by query_groups.group_index;

	 */

	auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
	std::cout << std::put_time(std::localtime(&now), "%c ") << "Benchmark initialized..."
	          << "\n";

	// generating groups
	CreateTable(scale_factor, percentage_insertions);
	std::cout << std::put_time(std::localtime(&now), "%c ") << "Generated groups data..."
	          << "\n";

	DuckDB db(nullptr);
	Connection con(db);
	con.Query("set ivm_files_path = 'data';");
	now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
	std::cout << std::put_time(std::localtime(&now), "%c ") << "Connected to the database instance..." << '\n';

	// loading the groups
	auto start_time = std::chrono::high_resolution_clock::now();
	con.Query("CREATE TABLE groups(group_index VARCHAR, group_value INTEGER);");

	con.Query("COPY groups FROM 'data/sf" + DoubleToString(scale_factor) + "/groups.tbl' (DELIMITER ',');");
	auto end_time = std::chrono::high_resolution_clock::now();
	auto load_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
	now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
	std::cout << std::put_time(std::localtime(&now), "%c ") << "Loaded groups..."
	          << "\n";
	auto count_groups = con.Query("SELECT COUNT(*) FROM groups;")->GetValue(0, 0).ToString();
	std::cout << std::put_time(std::localtime(&now), "%c ") << "Rows inserted in the base table: " << Format(count_groups) << "\n";

	// storing query 1 in a string for future usage
	// note: this is slightly different from query 1, since the average was removed
	string query_groups = "SELECT group_index, SUM(group_value) AS total_value FROM groups GROUP BY group_index;";
	string materialized_view = "CREATE MATERIALIZED VIEW query_groups AS " + query_groups;
	con.Query(materialized_view);

	auto system_queries = duckdb::CompilerExtension::ReadFile("data/ivm_system_tables.sql");
	auto ddl_queries = duckdb::CompilerExtension::ReadFile("data/ivm_compiled_queries_query_groups.sql");
	auto index_queries = duckdb::CompilerExtension::ReadFile("data/ivm_index_query_groups.sql");

	start_time = std::chrono::high_resolution_clock::now();
	con.Query(system_queries);
	con.Query(ddl_queries);
	end_time = std::chrono::high_resolution_clock::now();
	auto materialized_view_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
	now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
	std::cout << std::put_time(std::localtime(&now), "%c ") << "Materialized view created..."
	          << "\n";

	start_time = std::chrono::high_resolution_clock::now();
	con.Query(index_queries);
	end_time = std::chrono::high_resolution_clock::now();
	auto index_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
	now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
	std::cout << std::put_time(std::localtime(&now), "%c ") << "ART index created..." << "\n";

	auto count_query_groups = con.Query("SELECT COUNT(*) FROM query_groups;")->GetValue(0, 0).ToString();
	std::cout << std::put_time(std::localtime(&now), "%c ")
	          << "Rows inserted in the materialized view: " << Format(count_query_groups) << "\n";

	// inserting new data in the delta table
	con.Query("COPY delta_groups FROM 'data/sf" + DoubleToString(scale_factor) + "/groups_new_" +
	          to_string(percentage_insertions) + ".tbl' (DELIMITER ',');");
	now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
	std::cout << std::put_time(std::localtime(&now), "%c ") << "New data inserted..."
	          << "\n";
	auto count_delta_groups = con.Query("SELECT COUNT(*) FROM delta_groups;")->GetValue(0, 0).ToString();
	std::cout << std::put_time(std::localtime(&now), "%c ") << "Rows inserted in delta_groups: "
	          << Format(count_delta_groups) << "\n";

	// running the queries generates a file since our database is in memory
	// we want to see each query time individually -> we save the result to a file and then parse it
	start_time = std::chrono::high_resolution_clock::now();
	con.Query("PRAGMA ivm_upsert('memory', 'main', 'query_groups');");
	end_time = std::chrono::high_resolution_clock::now();
	auto compile_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
	now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
	std::cout << std::put_time(std::localtime(&now), "%c ") << "Queries compiled..."
	          << "\n";

	auto queries = ReadQueries("data/ivm_upsert_queries_query_groups.sql");
	auto insert_query = queries[0];
	auto upsert_query = queries[1];
	auto delete_query = queries[2];

	// now we insert
	start_time = std::chrono::high_resolution_clock::now();
	con.Query(insert_query);
	end_time = std::chrono::high_resolution_clock::now();
	auto insert_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
	now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
	std::cout << std::put_time(std::localtime(&now), "%c ") << "IVM insertion performed..."
	          << "\n";

	auto count_delta_query_groups = con.Query("SELECT COUNT(*) FROM delta_query_groups;")->GetValue(0, 0).ToString();
	// to make sure the cache is warm
	std::cout << std::put_time(std::localtime(&now), "%c ")
	          << "Rows inserted in the delta view: " << Format(count_delta_query_groups) << "\n";

	// joining query_groups with delta_query_groups
	std::cout << std::put_time(std::localtime(&now), "%c ")
	          << "Left join: " << Format(count_query_groups) << " x " << Format(count_delta_query_groups) << " rows" << "\n";
	// now we measure the performance of the join with and without adaptive radix tree
	auto join_query = ExtractSelect(upsert_query);
	start_time = std::chrono::high_resolution_clock::now();
	con.Query(join_query);
	end_time = std::chrono::high_resolution_clock::now();
	auto hash_join_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
	now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
	std::cout << std::put_time(std::localtime(&now), "%c ") << "Hash join performed..."
	          << "\n";

	// now we enable ART
	auto q = con.Query("PRAGMA force_index_join;");
	start_time = std::chrono::high_resolution_clock::now();
	con.Query(join_query);
	end_time = std::chrono::high_resolution_clock::now();
	auto art_join_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
	now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
	std::cout << std::put_time(std::localtime(&now), "%c ") << "Radix join performed..."
	          << "\n";

	start_time = std::chrono::high_resolution_clock::now();
	con.Query(upsert_query);
	end_time = std::chrono::high_resolution_clock::now();
	auto upsert_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
	now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
	std::cout << std::put_time(std::localtime(&now), "%c ") << "IVM upsert performed..."
	          << "\n";

	start_time = std::chrono::high_resolution_clock::now();
	con.Query(delete_query);
	end_time = std::chrono::high_resolution_clock::now();
	auto delete_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
	now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
	std::cout << std::put_time(std::localtime(&now), "%c ") << "IVM deletion performed..."
	          << "\n";

	auto new_count_query_groups = con.Query("SELECT COUNT(*) FROM query_groups;")->GetValue(0, 0).ToString();
	std::cout << std::put_time(std::localtime(&now), "%c ")
	          << "Rows in the materialized view after the upsert: " << Format(new_count_query_groups) << "\n";

	// finally, we measure the time to run the query without IVM
	// we copy the data first
	start_time = std::chrono::high_resolution_clock::now();
	// remove the multiplicity column
	con.Query("INSERT INTO groups SELECT group_index, group_value FROM delta_groups;");
	end_time = std::chrono::high_resolution_clock::now();
	auto copy_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
	now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
	std::cout << std::put_time(std::localtime(&now), "%c ") << "Data copy performed..."
	          << "\n";
	auto count_groups_new = con.Query("SELECT COUNT(*) FROM groups;")->GetValue(0, 0).ToString();
	std::cout << std::put_time(std::localtime(&now), "%c ") << "Total rows in the groups table: " << Format(count_groups_new) << "\n";

	start_time = std::chrono::high_resolution_clock::now();
	con.Query(query_groups);
	end_time = std::chrono::high_resolution_clock::now();
	auto query_groups_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
	now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
	std::cout << std::put_time(std::localtime(&now), "%c ") << "Query executed..."
	          << "\n";

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
	std::cout << "| " << std::left << std::setw(41) << "Queries compilation"
	          << "| " << std::setw(9) << compile_time.count() << " |" << '\n';
	std::cout << "| " << std::left << std::setw(41) << "Hash join"
	          << "| " << std::setw(9) << hash_join_time.count() << " |" << '\n';
	std::cout << "| " << std::left << std::setw(41) << "Radix join"
	          << "| " << std::setw(9) << art_join_time.count() << " |" << '\n';
	std::cout << "| " << std::left << std::setw(41) << "IVM insertion"
	          << "| " << std::setw(9) << insert_time.count() << " |" << '\n';
	std::cout << "| " << std::left << std::setw(41) << "IVM upsert"
	          << "| " << std::setw(9) << upsert_time.count() << " |" << '\n';
	std::cout << "| " << std::left << std::setw(41) << "IVM deletion"
	          << "| " << std::setw(9) << delete_time.count() << " |" << '\n';
	std::cout << "| " << std::left << std::setw(41) << "Data copy"
	          << "| " << std::setw(9) << copy_time.count() << " |" << '\n';
	std::cout << "| " << std::left << std::setw(41) << "Query 1 (without IVM)"
	          << "| " << std::setw(9) << query_groups_time.count() << " |" << '\n';
	std::cout << std::setw(56) << std::setfill('-') << "" << '\n';
}

} // namespace duckdb