#include "../compiler/include/compiler_extension.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "include/ivm_benchmark.hpp"

#include <cmath>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <random>

namespace duckdb {

void RunIVMCrossSystemBenchmark(double scale_factor, int percentage_insertions, int percentage_updates,
                                int percentage_deletes, BenchmarkType mode) {

	// usage: call ivm_benchmark('postgres', 1, 10, 10, 10);

	// two modes:
	// 1 - Postgres only
	// 2 - cross-system (generate and store the data in Postgres, then load it in DuckDB)

	// todo check max memory allocation
	// todo create temporary table

	auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
	std::cout << std::put_time(std::localtime(&now), "%c ") << "Benchmark initialized..."
	          << "\n";

	// generating groups
	CreateTable(scale_factor, percentage_insertions);
	std::cout << std::put_time(std::localtime(&now), "%c ") << "Generated groups data..."
	          << "\n";

	// initializing time variables
	std::chrono::milliseconds load_time;
	std::chrono::milliseconds materialized_view_time;
	std::chrono::milliseconds index_time;
	std::chrono::milliseconds compile_time;
	std::chrono::milliseconds join_time;
	std::chrono::milliseconds insert_time;
	std::chrono::milliseconds upsert_time;
	std::chrono::milliseconds delete_time;
	std::chrono::milliseconds copy_time;
	std::chrono::milliseconds query_groups_time;

	// translating the DuckDB API instruction to libpq
	const char *user = std::getenv("USER");
	string conn_info = "user=" + string(user);
	PGconn *conn = PQconnectdb(conn_info.c_str());

	if (PQstatus(conn) != CONNECTION_OK) {
		std::cout << "Connection to database failed: " << PQerrorMessage(conn) << std::endl;
		PQfinish(conn);
		return;
	}

	now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
	std::cout << std::put_time(std::localtime(&now), "%c ") << "Connected to the PostgreSQL database instance..."
	          << '\n';

	switch (mode) {
	case BenchmarkType::POSTGRES: {

		// loading the groups
		auto start_time = std::chrono::high_resolution_clock::now();
		auto r = PQresultErrorMessage(PQexec(conn, "CREATE TABLE groups(group_index VARCHAR, group_value INTEGER);"));
		if (r == nullptr) {
			std::cout << "Error creating the groups table: " << r << std::endl;
			break;
		}
		auto copy_groups = "COPY groups FROM '/tmp/data/sf" + DoubleToString(scale_factor) +
		                   "/groups.tbl' (DELIMITER ',');";
		r = PQresultErrorMessage(PQexec(conn, copy_groups.c_str()));
		auto end_time = std::chrono::high_resolution_clock::now();
		if (r == nullptr) {
			std::cout << "Error copying the groups table: " << r << std::endl;
			break;
		}
		load_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
		now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
		std::cout << std::put_time(std::localtime(&now), "%c ") << "Loaded groups..."
		          << "\n";

		auto count_groups = PQgetvalue(PQexec(conn, "SELECT COUNT(*) FROM groups;"), 0, 0);
		std::cout << std::put_time(std::localtime(&now), "%c ")
		          << "Rows inserted in the base table: " << Format(count_groups) << "\n";

		string query_groups = "SELECT group_index, SUM(group_value) AS total_value FROM groups GROUP BY group_index;";
		PQexec(conn, query_groups.c_str()); // to warm cache (will take a while)

		auto system_queries = duckdb::CompilerExtension::ReadFile("data/ivm_system_tables.sql");
		auto ddl_queries = duckdb::CompilerExtension::ReadFile("data/ivm_compiled_queries_query_groups.sql");
		auto index_queries = duckdb::CompilerExtension::ReadFile("data/ivm_index_query_groups.sql");

		start_time = std::chrono::high_resolution_clock::now();
		PQexec(conn, system_queries.c_str());
		PQexec(conn, ddl_queries.c_str());
		end_time = std::chrono::high_resolution_clock::now();
		materialized_view_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
		now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
		std::cout << std::put_time(std::localtime(&now), "%c ") << "Materialized view created..."
		          << "\n";

		start_time = std::chrono::high_resolution_clock::now();
		r = PQresultErrorMessage(PQexec(conn, index_queries.c_str()));
		end_time = std::chrono::high_resolution_clock::now();
		index_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
		now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());

		if (r == nullptr) {
			std::cout << "Error creating the index: " << r << std::endl;
			break;
		}

		std::cout << std::put_time(std::localtime(&now), "%c ") << "Index created..."
		          << "\n";

		auto count_query_groups = PQgetvalue(PQexec(conn, "SELECT COUNT(*) FROM query_groups;"), 0, 0);
		std::cout << std::put_time(std::localtime(&now), "%c ")
		          << "Rows inserted in the materialized view: " << Format(count_query_groups) << "\n";

		// inserting new data in the delta table
		auto copy_delta_groups = "COPY delta_groups FROM '/tmp/data/sf" + DoubleToString(scale_factor) + "/groups_new_" +
		                         to_string(percentage_insertions) + ".tbl' (DELIMITER ',');";
		r = PQresultErrorMessage(PQexec(conn, copy_delta_groups.c_str()));
		if (r == nullptr) {
			std::cout << "Error copying the delta groups table: " << r << std::endl;
			break;
		}

		now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
		std::cout << std::put_time(std::localtime(&now), "%c ") << "New data inserted..."
		          << "\n";
		auto count_delta_groups = PQgetvalue(PQexec(conn, "SELECT COUNT(*) FROM delta_groups;"), 0, 0);
		std::cout << std::put_time(std::localtime(&now), "%c ")
		          << "Rows inserted in delta_groups: " << Format(count_delta_groups) << "\n";

		auto queries = ReadQueries("data/ivm_upsert_queries_query_groups.sql");
		auto insert_query = queries[0];
		auto upsert_query = queries[1];
		auto delete_query = queries[2];

		// now we insert
		start_time = std::chrono::high_resolution_clock::now();
		r = PQresultErrorMessage(PQexec(conn, insert_query.c_str()));
		end_time = std::chrono::high_resolution_clock::now();
		insert_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
		now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
		if (r == nullptr) {
			std::cout << "Error inserting data: " << r << std::endl;
			break;
		}
		std::cout << std::put_time(std::localtime(&now), "%c ") << "IVM insertion performed..."
		          << "\n";

		auto count_delta_query_groups = PQgetvalue(PQexec(conn, "SELECT COUNT(*) FROM delta_query_groups;"), 0, 0);

		// to make sure the cache is warm
		std::cout << std::put_time(std::localtime(&now), "%c ")
		          << "Rows inserted in the delta view: " << Format(count_delta_query_groups) << "\n";

		// joining query_groups with delta_query_groups
		std::cout << std::put_time(std::localtime(&now), "%c ") << "Left join: " << Format(count_query_groups) << " x "
		          << Format(count_delta_query_groups) << " rows"
		          << "\n";
		// now we measure the performance of the join
		auto join_query = ExtractSelect(upsert_query);
		start_time = std::chrono::high_resolution_clock::now();
		PQexec(conn, join_query.c_str());
		end_time = std::chrono::high_resolution_clock::now();
		join_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
		now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
		std::cout << std::put_time(std::localtime(&now), "%c ") << "Join performed..."
		          << "\n";

		start_time = std::chrono::high_resolution_clock::now();
		r = PQresultErrorMessage(PQexec(conn, upsert_query.c_str()));
		end_time = std::chrono::high_resolution_clock::now();
		upsert_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
		if (r == nullptr) {
			std::cout << "Error upserting data: " << r << std::endl;
			break;
		}
		now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
		std::cout << std::put_time(std::localtime(&now), "%c ") << "IVM upsert performed..."
		          << "\n";

		start_time = std::chrono::high_resolution_clock::now();
		PQexec(conn, delete_query.c_str());
		end_time = std::chrono::high_resolution_clock::now();
		delete_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
		now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
		std::cout << std::put_time(std::localtime(&now), "%c ") << "IVM deletion performed..."
		          << "\n";

		auto new_count_query_groups = PQgetvalue(PQexec(conn, "SELECT COUNT(*) FROM query_groups;"), 0, 0);
		std::cout << std::put_time(std::localtime(&now), "%c ")
		          << "Rows in the materialized view after the upsert: " << Format(new_count_query_groups) << "\n";

		// finally, we measure the time to run the query without IVM
		// we copy the data first
		start_time = std::chrono::high_resolution_clock::now();
		// remove the multiplicity column
		PQexec(conn, "INSERT INTO groups SELECT group_index, group_value FROM delta_groups;");
		end_time = std::chrono::high_resolution_clock::now();
		copy_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
		now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
		std::cout << std::put_time(std::localtime(&now), "%c ") << "Data copy performed..."
		          << "\n";
		auto count_groups_new = PQgetvalue(PQexec(conn, "SELECT COUNT(*) FROM groups;"), 0, 0);
		std::cout << std::put_time(std::localtime(&now), "%c ")
		          << "Total rows in the groups table: " << Format(count_groups_new) << "\n";

		start_time = std::chrono::high_resolution_clock::now();
		PQexec(conn, query_groups.c_str());
		end_time = std::chrono::high_resolution_clock::now();
		query_groups_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
		now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
		std::cout << std::put_time(std::localtime(&now), "%c ") << "Query executed..."
		          << "\n";

	} break;
	case BenchmarkType::CROSS_SYSTEM: {

		// loading the groups
		auto start_time = std::chrono::high_resolution_clock::now();
		PQexec(conn, "CREATE TABLE groups(group_index VARCHAR, group_value INTEGER);");
		auto copy_groups = "COPY groups FROM 'data/sf" + DoubleToString(scale_factor) + "/groups.tbl' (DELIMITER ',');";
		PQexec(conn, copy_groups.c_str());
		auto end_time = std::chrono::high_resolution_clock::now();
		load_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
		now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
		std::cout << std::put_time(std::localtime(&now), "%c ") << "Loaded groups..."
		          << "\n";

		auto count_groups = PQgetvalue(PQexec(conn, "SELECT COUNT(*) FROM groups;"), 0, 0);
		std::cout << std::put_time(std::localtime(&now), "%c ")
		          << "Rows inserted in the base table: " << Format(count_groups) << "\n";

		DuckDB db(nullptr);
		Connection con(db);
		con.Query("ATTACH '' AS postgres_db (TYPE postgres);");
		now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
		std::cout << std::put_time(std::localtime(&now), "%c ") << "Connected to the DuckDB database instance..."
		          << '\n';

		string query_groups =
		    "SELECT group_index, SUM(group_value) AS total_value FROM postgres_db.groups GROUP BY group_index;";
		string materialized_view = "CREATE MATERIALIZED VIEW query_groups AS " + query_groups;
		con.Query(materialized_view);

		auto system_queries = duckdb::CompilerExtension::ReadFile("data/ivm_system_tables.sql");
		auto ddl_queries = duckdb::CompilerExtension::ReadFile("data/ivm_compiled_queries_query_groups.sql");
		auto index_queries = duckdb::CompilerExtension::ReadFile("data/ivm_index_query_groups.sql");

		start_time = std::chrono::high_resolution_clock::now();
		con.Query(system_queries);
		con.Query(ddl_queries);
		end_time = std::chrono::high_resolution_clock::now();
		materialized_view_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
		now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
		std::cout << std::put_time(std::localtime(&now), "%c ") << "Materialized view created..."
		          << "\n";

		start_time = std::chrono::high_resolution_clock::now();
		con.Query(index_queries);
		end_time = std::chrono::high_resolution_clock::now();
		index_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
		now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
		std::cout << std::put_time(std::localtime(&now), "%c ") << "Index created..."
		          << "\n";

		auto count_query_groups = con.Query("SELECT COUNT(*) FROM query_groups;")->GetValue(0, 0).ToString();
		std::cout << std::put_time(std::localtime(&now), "%c ")
		          << "Rows inserted in the materialized view: " << Format(count_query_groups) << "\n";

		// inserting new data in the delta table
		auto copy_delta_groups = "COPY delta_groups FROM 'data/sf" + DoubleToString(scale_factor) + "/groups_new_" +
		                         to_string(percentage_insertions) + ".tbl' (DELIMITER ',');";
		PQexec(conn, copy_delta_groups.c_str());

		now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
		std::cout << std::put_time(std::localtime(&now), "%c ") << "New data inserted..."
		          << "\n";
		auto count_delta_groups = PQgetvalue(PQexec(conn, "SELECT COUNT(*) FROM delta_groups;"), 0, 0);
		std::cout << std::put_time(std::localtime(&now), "%c ")
		          << "Rows inserted in delta_groups: " << Format(count_delta_groups) << "\n";

		// todo check table names here
		auto queries = ReadQueries("data/ivm_upsert_queries_query_groups.sql");
		auto insert_query = queries[0];
		auto upsert_query = queries[1];
		auto delete_query = queries[2];

		// now we insert
		start_time = std::chrono::high_resolution_clock::now();
		con.Query(insert_query);
		end_time = std::chrono::high_resolution_clock::now();
		insert_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
		now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
		std::cout << std::put_time(std::localtime(&now), "%c ") << "IVM insertion performed..."
		          << "\n";

		auto count_delta_query_groups =
		    con.Query("SELECT COUNT(*) FROM delta_query_groups;")->GetValue(0, 0).ToString();
		// to make sure the cache is warm
		std::cout << std::put_time(std::localtime(&now), "%c ")
		          << "Rows inserted in the delta view: " << Format(count_delta_query_groups) << "\n";

		// joining query_groups with delta_query_groups
		std::cout << std::put_time(std::localtime(&now), "%c ") << "Left join: " << Format(count_query_groups) << " x "
		          << Format(count_delta_query_groups) << " rows"
		          << "\n";
		// now we measure the performance of the join with and without adaptive radix tree
		auto join_query = ExtractSelect(upsert_query);
		con.Query("PRAGMA force_index_join;");
		start_time = std::chrono::high_resolution_clock::now();
		con.Query(join_query);
		end_time = std::chrono::high_resolution_clock::now();
		join_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
		now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
		std::cout << std::put_time(std::localtime(&now), "%c ") << "Join performed..."
		          << "\n";

		start_time = std::chrono::high_resolution_clock::now();
		con.Query(upsert_query);
		end_time = std::chrono::high_resolution_clock::now();
		upsert_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
		now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
		std::cout << std::put_time(std::localtime(&now), "%c ") << "IVM upsert performed..."
		          << "\n";

		start_time = std::chrono::high_resolution_clock::now();
		con.Query(delete_query);
		end_time = std::chrono::high_resolution_clock::now();
		delete_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
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
		copy_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
		now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
		std::cout << std::put_time(std::localtime(&now), "%c ") << "Data copy performed..."
		          << "\n";
		auto count_groups_new = con.Query("SELECT COUNT(*) FROM groups;")->GetValue(0, 0).ToString();
		std::cout << std::put_time(std::localtime(&now), "%c ")
		          << "Total rows in the groups table: " << Format(count_groups_new) << "\n";

		start_time = std::chrono::high_resolution_clock::now();
		con.Query(query_groups);
		end_time = std::chrono::high_resolution_clock::now();
		query_groups_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
		now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
		std::cout << std::put_time(std::localtime(&now), "%c ") << "Query executed..."
		          << "\n";
	} break;
	}

	// todo this drops everything, create a new schema instead
	//PQexec(conn, "DROP SCHEMA public CASCADE;");
	//PQexec(conn, "CREATE SCHEMA public;");
	PQfinish(conn);

	auto total_time = materialized_view_time + index_time + compile_time + join_time + insert_time + upsert_time +
	                  delete_time + copy_time;

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
	std::cout << "| " << std::left << std::setw(41) << "Index creation"
	          << "| " << std::setw(9) << index_time.count() << " |" << '\n';
	std::cout << "| " << std::left << std::setw(41) << "Queries compilation"
	          << "| " << std::setw(9) << compile_time.count() << " |" << '\n';
	std::cout << "| " << std::left << std::setw(41) << "Hash join"
	          << "| " << std::setw(9) << join_time.count() << " |" << '\n';
	std::cout << "| " << std::left << std::setw(41) << "IVM insertion"
	          << "| " << std::setw(9) << insert_time.count() << " |" << '\n';
	std::cout << "| " << std::left << std::setw(41) << "IVM upsert"
	          << "| " << std::setw(9) << upsert_time.count() << " |" << '\n';
	std::cout << "| " << std::left << std::setw(41) << "IVM deletion"
	          << "| " << std::setw(9) << delete_time.count() << " |" << '\n';
	std::cout << "| " << std::left << std::setw(41) << "Data copy"
	          << "| " << std::setw(9) << copy_time.count() << " |" << '\n';
	std::cout << "| " << std::left << std::setw(41) << "Total (IVM)"
	          << "| " << std::setw(9) << total_time.count() << " |" << '\n';
	std::cout << "| " << std::left << std::setw(41) << "Query 1 (without IVM)"
	          << "| " << std::setw(9) << query_groups_time.count() << " |" << '\n';
	std::cout << std::setw(56) << std::setfill('-') << "" << '\n';
}

} // namespace duckdb