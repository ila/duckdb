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

// Function to generate and append new "lineitem" data with a different random seed and insertions
void GenerateLineitem(double scale_factor, double percentage_insertions) {

	LocalFileSystem fs;

	// todo gitignore?
	// todo dbgen path?
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
		string dbgen_command_original = "./dbgen -T L -q -s " + DoubleToString(scale_factor);
		int result_original = std::system(dbgen_command_original.c_str());

		if (result_original != 0) {
			throw std::runtime_error("Error running dbgen for original lineitem data with scale factor " +
			                         DoubleToString(scale_factor));
		}
		// move the generated file to the correct directory
		fs.MoveFile("lineitem.tbl", lineitem_file);
	}

	// generate new lineitem data with insertions using dbgen with a different random seed
	double new_scale_factor = scale_factor * percentage_insertions / 100;

	string new_lineitem_file = scale_dir + "lineitem_new_" + DoubleToString(percentage_insertions) + ".tbl";

	if (!fs.FileExists(new_lineitem_file)) {
		// todo random seed?
		string dbgen_command_new = "./dbgen -T L -q -s " + to_string(new_scale_factor);
		int result_new = std::system(dbgen_command_new.c_str());

		if (result_new != 0) {
			throw std::runtime_error("Error running dbgen for new lineitem data with insertions for scale factor " +
			                         DoubleToString(scale_factor));
		}
		// move the generated file to the correct directory
		fs.MoveFile("lineitem.tbl", new_lineitem_file);
	}
}

void RunIVMLineitemBenchmark(double scale_factor, int percentage_insertions, int percentage_updates, int percentage_deletes) {

	// usage: call ivm_benchmark('lineitem', 1, 10, 10, 10);
	// todo: fix all the paths (this only works copying dists.dss in the debug folder)

	auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
	std::cout << std::put_time(std::localtime(&now), "%c ") << "Benchmark initialized..."
	          << "\n";

	// generating lineitem
	GenerateLineitem(scale_factor, percentage_insertions);
	std::cout << std::put_time(std::localtime(&now), "%c ") << "Generated lineitem..."
	          << "\n";

	DuckDB db(nullptr);
	Connection con(db);
	con.Query("set ivm_files_path = 'data';");
	now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
	std::cout << std::put_time(std::localtime(&now), "%c ") << "Connected to the database instance..." << '\n';

	// loading lineitem
	auto start_time = std::chrono::high_resolution_clock::now();
	con.Query("CREATE TABLE lineitem(l_orderkey INTEGER, l_partkey INTEGER, l_suppkey INTEGER, l_linenumber INTEGER, "
	          "l_quantity DECIMAL(15,2), l_extendedprice DECIMAL(15,2), l_discount DECIMAL(15,2), l_tax DECIMAL(15,2), "
	          "l_returnflag VARCHAR(1), l_linestatus VARCHAR(1), l_shipdate DATE, l_commitdate DATE, l_receiptdate "
	          "DATE, l_shipinstruct VARCHAR(25), l_shipmode VARCHAR(10), l_comment VARCHAR(44));");

	con.Query("COPY lineitem FROM 'data/sf" + DoubleToString(scale_factor) + "/lineitem.tbl' (DELIMITER '|');");
	auto end_time = std::chrono::high_resolution_clock::now();
	auto load_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
	now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
	std::cout << std::put_time(std::localtime(&now), "%c ") << "Loaded lineitem..."
	          << "\n";
	auto count_lineitem = con.Query("SELECT COUNT(*) FROM lineitem;")->GetValue(0, 0).ToString();
	std::cout << std::put_time(std::localtime(&now), "%c ") << "Rows inserted in lineitem: " << Format(count_lineitem) << "\n";

	// storing query 1 in a string for future usage
	// note: this is slightly different from query 1, since the average was removed
	string query_1_tpch = "SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, SUM(l_extendedprice) AS "
	                      "sum_base_price, COUNT(*) AS count_order FROM lineitem WHERE l_shipdate <= DATE '1998-12-01' "
	                      "GROUP BY l_returnflag, l_linestatus;";

	string materialized_view = "CREATE MATERIALIZED VIEW query_1 AS " + query_1_tpch;
	con.Query(materialized_view); // this generates the files

	auto system_queries = duckdb::CompilerExtension::ReadFile("data/ivm_system_tables.sql");
	auto ddl_queries = duckdb::CompilerExtension::ReadFile("data/ivm_compiled_queries_query_1.sql");
	auto index_queries = duckdb::CompilerExtension::ReadFile("data/ivm_index_query_1.sql");

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

	auto count_query_1 = con.Query("SELECT COUNT(*) FROM query_1;")->GetValue(0, 0).ToString();
	std::cout << std::put_time(std::localtime(&now), "%c ")
	          << "Rows inserted in the materialized view: " << Format(count_query_1) << "\n";

	// inserting new data in the delta table
	con.Query("COPY delta_lineitem FROM 'data/sf" + DoubleToString(scale_factor) + "/lineitem_new_" +
	          to_string(percentage_insertions) + ".tbl' (DELIMITER '|');");
	// todo multiplicity column
	con.Query("UPDATE delta_lineitem SET _duckdb_ivm_multiplicity = true;");
	now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
	std::cout << std::put_time(std::localtime(&now), "%c ") << "New data inserted..."
	          << "\n";
	auto count_delta_lineitem = con.Query("SELECT COUNT(*) FROM delta_lineitem;")->GetValue(0, 0).ToString();
	std::cout << std::put_time(std::localtime(&now), "%c ") << "Rows inserted in delta_lineitem: "
	          << Format(count_delta_lineitem) << "\n";

	// running the queries generates a file since our database is in memory
	// we want to see each query time individually -> we save the result to a file and then parse it
	start_time = std::chrono::high_resolution_clock::now();
	con.Query("PRAGMA ivm_upsert('memory', 'main', 'query_1');");
	end_time = std::chrono::high_resolution_clock::now();
	auto compile_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
	now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
	std::cout << std::put_time(std::localtime(&now), "%c ") << "Queries compiled..."
	          << "\n";

	auto queries = ReadQueries("data/ivm_upsert_queries_query_1.sql");
	auto insert_query = queries[0];
	auto upsert_query = queries[1];
	auto delete_query = queries[2];

	// now we finally insert
	start_time = std::chrono::high_resolution_clock::now();
	auto res = con.Query(insert_query);
	end_time = std::chrono::high_resolution_clock::now();
	auto insert_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
	now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
	std::cout << std::put_time(std::localtime(&now), "%c ") << "IVM insertion performed..."
	          << "\n";

	auto count_delta_query_1 = con.Query("SELECT COUNT(*) FROM delta_query_1;")->GetValue(0, 0).ToString();
	// to make sure the cache is warm
	std::cout << std::put_time(std::localtime(&now), "%c ")
	          << "Rows inserted in the delta view: " << Format(count_delta_query_1) << "\n";

	std::cout << std::put_time(std::localtime(&now), "%c ")
	          << "Left join: " << Format(count_query_1) << " x " << Format(count_delta_query_1) << " rows" << "\n";

	// now we measure the performance of the join with and without adaptive radix tree
	auto join_query = ExtractSelect(upsert_query);
	con.Query("SELECT COUNT(*) FROM delta_lineitem;"); // to make sure the cache is warm
	start_time = std::chrono::high_resolution_clock::now();
	con.Query(join_query);
	end_time = std::chrono::high_resolution_clock::now();
	auto hash_join_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
	now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
	std::cout << std::put_time(std::localtime(&now), "%c ") << "Hash join performed..."
	          << "\n";

	// now we enable ART
	con.Query("PRAGMA force_index_join;");
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

	auto new_count_query_1 = con.Query("SELECT COUNT(*) FROM query_1;")->GetValue(0, 0).ToString();
	std::cout << std::put_time(std::localtime(&now), "%c ")
	          << "Rows in the materialized view after the upsert: " << Format(new_count_query_1) << "\n";

	// finally, we measure the time to run the query without IVM
	// we copy the data first
	start_time = std::chrono::high_resolution_clock::now();
	// remove the multiplicity column
	string lineitem_columns =
	    "l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, "
	    "l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment";
	con.Query("INSERT INTO lineitem SELECT " + lineitem_columns + " FROM delta_lineitem;");
	end_time = std::chrono::high_resolution_clock::now();
	auto copy_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
	now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
	std::cout << std::put_time(std::localtime(&now), "%c ") << "Data copy performed..."
	          << "\n";
	auto count_lineitem_new = con.Query("SELECT COUNT(*) FROM lineitem;")->GetValue(0, 0).ToString();
	std::cout << std::put_time(std::localtime(&now), "%c ") << "Total rows in lineitem: " << Format(count_lineitem_new) << "\n";

	start_time = std::chrono::high_resolution_clock::now();
	con.Query(query_1_tpch);
	end_time = std::chrono::high_resolution_clock::now();
	auto query_1_time = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
	now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
	std::cout << std::put_time(std::localtime(&now), "%c ") << "Query 1 executed..."
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
	          << "| " << std::setw(9) << query_1_time.count() << " |" << '\n';
	std::cout << std::setw(56) << std::setfill('-') << "" << '\n';
}

} // namespace duckdb