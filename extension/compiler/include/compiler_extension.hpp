#pragma once

#include "duckdb.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>

namespace duckdb {

class CompilerExtension : public Extension {
public:
	// this extension does not do anything in practice; it is a collection of functions to parse/compile SQL strings
	void Load(DuckDB &db) override;
	string Name() override;
	static void WriteFile(const string &filename, bool append, const string &compiled_query);
	static string ReadFile(const string& file_path);
	static string ExtractTableName(const string &sql);
	static string EscapeSingleQuotes(const string &input);
	static void ReplaceMaterializedView(string &query);
	static string ExtractViewQuery(string &query);
	static string ExtractViewName(const string &query);
	static string SQLToLowercase(const string &sql);
	static void ReplaceCount(string &query);
	static void ReplaceSum(string &query);
};

} // namespace duckdb
