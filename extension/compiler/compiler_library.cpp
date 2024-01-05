#include "include/compiler_library.hpp"
#include "../../extension/ivm/include/ivm_upsert.hpp"

#include "duckdb/common/local_file_system.hpp"
#include "include/compiler_extension.hpp"

#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>

SQLCompiler::SQLCompiler(const std::string& schema_file, const std::string& materialized_view_file, bool apply_updates)
    : schema_file(schema_file), materialized_view_file(materialized_view_file), apply_updates(apply_updates) {
}

void SQLCompiler::Compile() {

}

std::string ExtractParentFolder(const std::string& file_path) {
	size_t last_slash = file_path.find_last_of('/');

	if (last_slash != std::string::npos) {
		return file_path.substr(0, last_slash);
	} else {
		// No slash found, consider the current directory
		return ".";
	}
}

class FileNotFoundException : public std::exception {
public:
	const char* what() const throw() override {
		return "File not found.";
	}
};

int main(int argc, char* argv[]) {

	// export database:
	// EXPORT DATABASE '../test_sales' (FORMAT CSV);

	// cmake-build-debug/extension/compiler/compiler -s test_sales/schema.sql -m test_sales/materialized_view.sql

	std::string schema_file = "";
	std::string db_file = "";
	std::string materialized_view_file = "";
	bool apply_updates = false;
	// default engines and dialect (for now)
	std::string database_engine = "duckdb";
	std::string sql_dialect = "duckdb";

	if (argc < 2) {
		std::cerr << "Usage: " << argv[0] << " [-s <schema-file> | -d <db-file>] [-m <materialized-view-file>] [-u] [-e <database-engine>] [-q <sql-dialect>]" << std::endl;
		return 1;
	}

	for (int i = 1; i < argc; i++) {
		std::string arg = argv[i];
		if (arg == "-s") {
			if (i + 1 < argc) {
				schema_file = argv[i + 1];
				i++;
			} else {
				std::cerr << "Error: Missing schema file path." << '\n';
				return 1;
			}
		} else if (arg == "-d") {
			if (i + 1 < argc) {
				db_file = argv[i + 1];
				i++;
			} else {
				std::cerr << "Error: Missing database file path." << '\n';
				return 1;
			}
		} else if (arg == "-m") {
			if (i + 1 < argc) {
				materialized_view_file = argv[i + 1];
				i++;
			} else {
				std::cerr << "Error: Missing materialized view file path." << '\n';
				return 1;
			}
		} else if (arg == "-u") {
			apply_updates = true;
		} else if (arg == "-e") {
			if (i + 1 < argc) {
				database_engine = argv[i + 1];
				i++;
			} else {
				std::cerr << "Error: Missing database engine name." << '\n';
				return 1;
			}
		} else if (arg == "-q") {
			if (i + 1 < argc) {
				sql_dialect = argv[i + 1];
				i++;
			} else {
				std::cerr << "Error: Missing SQL dialect." << '\n';
				return 1;
			}
		}
	}

	try {
		// Check if the files exist and throw exceptions if not
		duckdb::LocalFileSystem fs;
		if (!schema_file.empty() && !fs.FileExists(schema_file)) {
			throw FileNotFoundException();
		}
		if (!db_file.empty() && !fs.FileExists(db_file)) {
			throw FileNotFoundException();
		}
		if (!materialized_view_file.empty() && !fs.FileExists(materialized_view_file)) {
			throw FileNotFoundException();
		}

		if (schema_file.empty() && db_file.empty()) {
			std::cerr << "Error: Either the schema SQL file or the database file is mandatory." << '\n';
			return 1;
		} else if (!schema_file.empty() && !db_file.empty()) {
			std::cerr << "Error: You cannot specify both the schema file and the database file." << '\n';
			return 1;
		}

		// if we have a .sql file, chances are there is also a .load file and some csv files with the data
		// todo - should do anything with the load?

		if (!materialized_view_file.empty()) {
			// we need to create a materialized view using either the schema or the database file
			// if only schema file is supplied, we open a connection in memory and do not apply changes
			// if we have a database, we create the materialized view there
			duckdb::DuckDB db(db_file);
			duckdb::Connection con(db);

			if (!schema_file.empty()) {
				auto schema = duckdb::CompilerExtension::ReadFile(schema_file);
				// we set the path of the generated files to be the same as the schema file
				// completely arbitrary decision [will change if it causes problems]
				std::string schema_file_folder = ExtractParentFolder(schema_file);
				con.Query("set ivm_files_path = '" + schema_file_folder + "';");
				con.Query(schema);
			}

			if (!materialized_view_file.empty()) {
				// we emulate the call of DoIVM and transform the logical plan to string
				// view catalog name, view schema name, view name
				// todo dynamic schema
				auto materialized_view = duckdb::CompilerExtension::ReadFile(materialized_view_file);
				con.Query(materialized_view);
				auto res = con.Query("pragma ivm_upsert('memory', 'main', 'product_sales');");
			}

		}

	} catch (const FileNotFoundException& e) {
		std::cerr << "File not found: " << e.what() << '\n';
		return 1;
	} catch (const std::exception& e) {
		std::cerr << "Error: " << e.what() << '\n';
		return 1;
	}

	return 0;
}
