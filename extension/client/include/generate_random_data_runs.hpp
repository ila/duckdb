//
// Created by ila on 4/9/25.
//

#ifndef GENERATE_RANDOM_DATA_RUNS_HPP
#define GENERATE_RANDOM_DATA_RUNS_HPP

#include <string>
#include <vector>
#include <duckdb.hpp>

namespace duckdb {
// A constant list of cities for random selection
extern const std::vector<std::string> cities;

// Returns a random element from a given list
std::string GetRandomElement(const std::vector<std::string> &list);

// Reads or generates a stable identity (nickname, city) from a file
std::string GetID(const std::string &filename);

// Returns and increments the current run count stored in a file
int GetRunCount(const std::string &filename);

// Formats a date string (YYYY-MM-DD) offset by a number of days
std::string FormatDate(int offset_days);

// Generates a random time string (HH:MM:SS) for run start/end times
std::string FormatTime();

// Appends a random number (1–5) of CSV-formatted rows to the given file
void GenerateCSV(const std::string &filename);

// Stores test data in DuckDB
void StoreTestData(ClientContext &context, const FunctionParameters &parameters);
} // namespace duckdb

#endif // GENERATE_RANDOM_DATA_RUNS_HPP
