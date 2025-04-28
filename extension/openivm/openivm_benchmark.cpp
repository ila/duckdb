#include "include/openivm_benchmark.hpp"

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

std::mt19937 generator(std::random_device {}());

string Format(const string &number_str) {
	std::string formatted_number;

	if (!number_str.empty() && std::isdigit(number_str[0])) {
		int length = number_str.length();
		formatted_number = number_str;

		if (length > 3) {
			// Insert space every three digits from the end
			for (int i = length - 3; i > 0; i -= 3) {
				formatted_number.insert(i, "'");
			}
		}
	} else {
		formatted_number = "Invalid input";
	}

	return formatted_number;
}

string DoubleToString(double value) {
	std::stringstream ss;
	ss << std::fixed << value; // Convert double to string

	std::string result = ss.str();

	// Check if the number is an integer (has no decimal part)
	size_t dot_position = result.find('.');
	if (dot_position != std::string::npos) {
		// Trim trailing zeros after the decimal point
		result.erase(result.find_last_not_of('0') + 1);

		// Remove the decimal point if all decimal places are zeros
		if (result.back() == '.') {
			result.pop_back();
		}
	}

	return result;
}

string ExtractSelect(const string &input_query) {
	size_t with_pos = input_query.find("with");
	if (with_pos == std::string::npos) {
		throw std::runtime_error("Error: 'with' clause not found in the input query.");
	}

	// extract the substring starting from "with" to the end of the input query
	std::string cte = input_query.substr(with_pos);

	return cte;
}

// Function to read a file and split its content into six strings
vector<string> ReadQueries(const string &filename) {
	std::ifstream file(filename);

	if (!file.is_open()) {
		throw std::runtime_error("Error opening file: " + filename);
	}

	vector<string> result;
	std::stringstream buffer;
	string line;

	while (std::getline(file, line)) {
		buffer << line << '\n';
	}

	file.close();

	// get the content of the stringstream
	std::string content = buffer.str();

	// find the positions of '\n\n' to split the content
	size_t pos1 = content.find("\n\n");
	size_t pos2 = content.find("\n\n", pos1 + 1);
	size_t pos3 = content.find("\n\n", pos2 + 1);
	size_t pos4 = content.find("\n\n", pos3 + 1);
	size_t pos5 = content.find("\n\n", pos4 + 1);
	size_t pos6 = content.find("\n\n", pos5 + 1);

	// Split the content into six strings
	result.push_back(content.substr(0, pos1));
	result.push_back(content.substr(pos1 + 2, pos2 - pos1 - 2));
	result.push_back(content.substr(pos2 + 2, pos3 - pos2 - 2));
	result.push_back(content.substr(pos3 + 2, pos4 - pos3 - 2));
	result.push_back(content.substr(pos4 + 2, pos5 - pos4 - 2));
	result.push_back(content.substr(pos5 + 2, pos6 - pos5 - 2));

	return result;
}

int GetRandomValue(size_t max_value) {
	std::uniform_int_distribution<int> distribution(1, static_cast<int>(max_value));
	return distribution(generator);
}

void CreateTable(int tuples, int insertions) {

	string data_dir = "/tmp/data/";
	string scale_dir = data_dir + "t" + DoubleToString(tuples) + "/";

	auto groups_path = scale_dir + "groups.tbl";
	auto groups_path_new = scale_dir + "groups_new_" + to_string(insertions) + ".tbl";

	LocalFileSystem fs;

	if (!fs.DirectoryExists(data_dir)) {
		fs.CreateDirectory(data_dir);
	}

	if (!fs.DirectoryExists(scale_dir)) {
		fs.CreateDirectory(scale_dir);
	}

	if (!fs.FileExists(groups_path)) {
		std::ofstream outfile(groups_path);

		for (int i = 1; i <= tuples; ++i) {
			outfile << i << ",Group_" << (i % (tuples / 3)) + 1 << "," << GetRandomValue() << '\n';
		}
	}

	if (!fs.FileExists(groups_path_new)) {
		std::ofstream outfile(groups_path_new);

		for (int i = 0; i < insertions - 100; ++i) {
			outfile << tuples + i << ",Group_" << (i % 100) << "," << GetRandomValue() << "\n";
		}
		for (int i = 0; i < 100; ++i) {
			outfile << "0,Group_xxxxx," << GetRandomValue() << "\n";
		}
	}
}

} // namespace duckdb
