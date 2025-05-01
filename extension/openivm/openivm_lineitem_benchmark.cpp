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

bool create_lineitem = true;
bool ivm_lineitem = true;

// Function to generate and append new "lineitem" data with a different random seed and insertions
void GenerateLineitem(double scale_factor, double new_scale_factor) {

	LocalFileSystem fs;

	string dbgen_path = "/home/ila/Code/TPC-H/dbgen/";
	string data_dir = "/tmp/data/";
	string scale_dir = data_dir + "sf" + DoubleToString(scale_factor) + "/";

	if (!fs.DirectoryExists(data_dir)) {
		fs.CreateDirectory(data_dir);
	}

	if (!fs.DirectoryExists(scale_dir)) {
		fs.CreateDirectory(scale_dir);
	}

	// generate the original lineitem data using dbgen
	string lineitem_file = scale_dir + "lineitem.tbl";
	if (!fs.FileExists(lineitem_file)) {
		string dbgen_command_original = dbgen_path + "dbgen -T L -q -s " + DoubleToString(scale_factor);
		int result_original = std::system(dbgen_command_original.c_str());

		if (result_original != 0) {
			throw std::runtime_error("Error running dbgen for lineitem data with scale factor " +
			                         DoubleToString(scale_factor));
		}
		// move the generated file to the correct directory
		// first remove the last character from the file
		system("sed -i 's/|$//' lineitem.tbl");
		fs.MoveFile("lineitem.tbl", lineitem_file);
	}

	// generate new lineitem data with insertions using dbgen with a different random seed

	string new_lineitem_file = scale_dir + "lineitem_new_" + DoubleToString(new_scale_factor) + ".tbl";

	if (!fs.FileExists(new_lineitem_file)) {
		string dbgen_command_new = dbgen_path + "dbgen -T L -q -s " + to_string(new_scale_factor);
		int result_new = std::system(dbgen_command_new.c_str());

		if (result_new != 0) {
			throw std::runtime_error("Error running dbgen for new lineitem data for scale factor " +
			                         DoubleToString(scale_factor));
		}
		// move the generated file to the correct directory
		system("sed -i 's/|$//' lineitem.tbl");
		fs.MoveFile("lineitem.tbl", new_lineitem_file);
	}
}

void RunIVMLineitemBenchmark(double scale_factor, double new_scale_factor) {

	// usage: call ivm_benchmark('lineitem', 1, 1);

	LocalFileSystem fs;
	fs.CreateDirectory("data");

	auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
	std::cout << std::put_time(std::localtime(&now), "%c ") << "Benchmark initialized..."
	          << "\n";

	std::cout << std::put_time(std::localtime(&now), "%c ") << "Generating data..."
	          << "\n";

	// generating lineitem
	GenerateLineitem(scale_factor, new_scale_factor);
	std::cout << std::put_time(std::localtime(&now), "%c ") << "Generated lineitem..."
	          << "\n";

	DuckDB db("lineitem.db");
	Connection con(db);
	con.Query("set ivm_files_path = 'data';");
	now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
	std::cout << std::put_time(std::localtime(&now), "%c ") << "Connected to the database instance..." << '\n';

	std::chrono::milliseconds load_time = std::chrono::milliseconds(0);

	// loading lineitem
	if (create_lineitem) {
		auto start_time = std::chrono::high_resolution_clock::now();
		con.Query(
		    "CREATE TABLE lineitem(l_orderkey INTEGER, l_partkey INTEGER, l_suppkey INTEGER, l_linenumber INTEGER, "
		    "l_quantity DECIMAL(15,2), l_extendedprice DECIMAL(15,2), l_discount DECIMAL(15,2), l_tax DECIMAL(15,2), "
		    "l_returnflag VARCHAR(1), l_linestatus VARCHAR(1), l_shipdate DATE, l_commitdate DATE, l_receiptdate "
		    "DATE, l_shipinstruct VARCHAR(25), l_shipmode VARCHAR(10), l_comment VARCHAR(44));");

		std::cout << std::put_time(std::localtime(&now), "%c ") << "Loading data..."
		          << "\n";

		auto r = con.Query("COPY lineitem FROM '/tmp/data/sf" + DoubleToString(scale_factor) +
		                   "/lineitem.tbl' (DELIMITER '|');");
		if (r->HasError()) {
			fs.RemoveFile("lineitem.db");
			throw InternalException("Failed to load lineitem data: %s", r->GetError().c_str());
		}
		auto end_time = std::chrono::high_resolution_clock::now();
		load_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
		now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
		std::cout << std::put_time(std::localtime(&now), "%c ") << "Loaded lineitem..."
		          << "\n";
	}
	auto count_lineitem = con.Query("SELECT COUNT(*) FROM lineitem;")->GetValue(0, 0).ToString();
	std::cout << std::put_time(std::localtime(&now), "%c ") << "Rows inserted in lineitem: " << Format(count_lineitem)
	          << "\n";

	// storing query 1 in a string for future usage
	// note: this is slightly different from query 1, since the average was removed
	string query_1_tpch =
	    "SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, SUM(l_extendedprice) AS "
	    "sum_base_price, COUNT(*) AS count_order FROM lineitem WHERE l_shipdate <= CAST('1998-09-02' AS date) "
	    "GROUP BY l_returnflag, l_linestatus;";

	string materialized_view = "CREATE MATERIALIZED VIEW query_1 AS " + query_1_tpch;
	std::chrono::milliseconds materialized_view_time = std::chrono::milliseconds(0);
	std::chrono::milliseconds index_time = std::chrono::milliseconds(0);

	if (create_lineitem) {

		con.Query("set execute = false");
		auto r = con.Query(materialized_view); // this generates the files
		if (r->HasError()) {
			fs.RemoveFile("lineitem.db");
			throw InternalException("Failed to create materialized view: %s", r->GetError().c_str());
		}

		auto system_queries = duckdb::CompilerExtension::ReadFile("data/ivm_system_tables.sql");
		auto ddl_queries = duckdb::CompilerExtension::ReadFile("data/ivm_compiled_queries_query_1.sql");
		auto index_queries = duckdb::CompilerExtension::ReadFile("data/ivm_index_query_1.sql");

		auto start_time = std::chrono::high_resolution_clock::now();
		r = con.Query(system_queries);
		if (r->HasError()) {
			fs.RemoveFile("lineitem.db");
			throw InternalException("Failed to create system tables: %s", r->GetError().c_str());
		}
		r = con.Query(ddl_queries);
		if (r->HasError()) {
			fs.RemoveFile("lineitem.db");
			throw InternalException("Failed to create compiled queries: %s", r->GetError().c_str());
		}
		auto end_time = std::chrono::high_resolution_clock::now();
		materialized_view_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
		now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
		std::cout << std::put_time(std::localtime(&now), "%c ") << "Materialized view created..."
		          << "\n";

		std::cout << std::put_time(std::localtime(&now), "%c ") << "Query: " << query_1_tpch << "\n";

		start_time = std::chrono::high_resolution_clock::now();
		con.Query(index_queries);
		end_time = std::chrono::high_resolution_clock::now();
		index_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
		now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
		std::cout << std::put_time(std::localtime(&now), "%c ") << "ART index created..."
		          << "\n";

		create_lineitem = false;
	}

	auto count_query_1 = con.Query("SELECT COUNT(*) FROM query_1;")->GetValue(0, 0).ToString();
	std::cout << std::put_time(std::localtime(&now), "%c ")
	          << "Rows inserted in the materialized view: " << Format(count_query_1) << "\n";

	// inserting new data in the delta table
	std::cout << std::put_time(std::localtime(&now), "%c ") << "Applying modifications to the base table..."
	          << "\n";
	auto start_time = std::chrono::high_resolution_clock::now();
	auto r = con.Query("COPY lineitem FROM '/tmp/data/sf" + DoubleToString(scale_factor) + "/lineitem_new_" +
	                   DoubleToString(new_scale_factor) + ".tbl' (DELIMITER '|');");
	if (r->HasError()) {
		fs.RemoveFile("lineitem.db");
		throw InternalException("Failed to load new lineitem data: %s", r->GetError().c_str());
	}
	auto end_time = std::chrono::high_resolution_clock::now();
	auto copy_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

	now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
	std::cout << std::put_time(std::localtime(&now), "%c ") << "New data inserted..."
	          << "\n";
	auto count_delta_lineitem = con.Query("SELECT COUNT(*) FROM delta_lineitem;")->GetValue(0, 0).ToString();
	std::cout << std::put_time(std::localtime(&now), "%c ")
	          << "Rows inserted in delta_lineitem: " << Format(count_delta_lineitem) << "\n";

	// running the queries generates a file since our database is in memory
	// we want to see each query time individually -> we save the result to a file and then parse it
	start_time = std::chrono::high_resolution_clock::now();
	if (ivm_lineitem) {
		r = con.Query("PRAGMA ivm('query_1');");
		if (r->HasError()) {
			fs.RemoveFile("lineitem.db");
			throw InternalException("Failed to run the query: %s", r->GetError().c_str());
		}
		ivm_lineitem = false;
	}
	end_time = std::chrono::high_resolution_clock::now();
	auto compile_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
	now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
	std::cout << std::put_time(std::localtime(&now), "%c ") << "Queries compiled..."
	          << "\n";

	auto queries = ReadQueries("ivm_upsert_queries_query_1.sql");
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
		fs.RemoveFile("lineitem.db");
		throw InternalException("Failed to insert data: %s", r->GetError().c_str());
	}
	end_time = std::chrono::high_resolution_clock::now();
	auto insert_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
	now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
	std::cout << std::put_time(std::localtime(&now), "%c ") << "IVM insertion performed..."
	          << "\n";

	auto count_delta_query_1 = con.Query("SELECT COUNT(*) FROM delta_query_1;")->GetValue(0, 0).ToString();
	// to make sure the cache is warm
	std::cout << std::put_time(std::localtime(&now), "%c ")
	          << "Rows inserted in delta_query_1: " << Format(count_delta_query_1) << "\n";

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

	auto new_count_query_1 = con.Query("SELECT COUNT(*) FROM query_1")->GetValue(0, 0).ToString();
	std::cout << std::put_time(std::localtime(&now), "%c ")
	          << "Rows in the materialized view after the upsert: " << Format(new_count_query_1) << "\n";

	// finally, we measure the time to run the query without IVM
	auto count_lineitem_new = con.Query("SELECT COUNT(*) FROM lineitem;")->GetValue(0, 0).ToString();
	std::cout << std::put_time(std::localtime(&now), "%c ")
	          << "Total rows in the lineitem table: " << Format(count_lineitem_new) << "\n";

	start_time = std::chrono::high_resolution_clock::now();
	con.Query(query_1_tpch);
	end_time = std::chrono::high_resolution_clock::now();
	auto query_lineitem_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
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
	          << "| " << std::setw(9) << query_lineitem_time.count() << " |" << '\n';
	std::cout << std::setw(56) << std::setfill('-') << "" << '\n';
}

} // namespace duckdb
