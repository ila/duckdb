#ifndef IVM_BENCHMARK_HPP
#define IVM_BENCHMARK_HPP

#include "duckdb.hpp"
#include <chrono>
#include <iostream>
#include <libpq-fe.h>

namespace duckdb {

enum class BenchmarkType { POSTGRES, CROSS_SYSTEM };

int GetRandomValue();
void CreateTable(double scale_factor, int insertions);
string DoubleToString(double value);
string Format(const string& number_str);
string ExtractSelect(const string &input_query);
void GenerateLineitem(double scale_factor, double percentage_insertions);
vector<string> ReadQueries(const string &filename);
void RunIVMLineitemBenchmark(double scale_factor, int insert_pct, int update_pct, int delete_pct);
void RunIVMGroupsBenchmark(double scale_factor, int insert_pct, int update_pct, int delete_pct);
void RunIVMCrossSystemBenchmark(double scale_factor, int insert_pct, int update_pct, int delete_pct,
								BenchmarkType benchmark_type);

} // namespace duckdb

#endif

