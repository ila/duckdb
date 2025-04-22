#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <ctime>
#include <cstdlib>
#include <random>
#include <filesystem>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/database.hpp>

namespace duckdb {
const std::vector<std::string> cities = {
    "New York",      "Los Angeles",    "Chicago",   "Houston",          "Phoenix",      "Philadelphia",
    "San Antonio",   "San Diego",      "Dallas",    "San Jose",         "Austin",       "Jacksonville",
    "Fort Worth",    "Columbus",       "Charlotte", "San Francisco",    "Indianapolis", "Seattle",
    "Denver",        "Washington",     "Boston",    "El Paso",          "Nashville",    "Detroit",
    "Oklahoma City", "Portland",       "Las Vegas", "Memphis",          "Louisville",   "Baltimore",
    "Milwaukee",     "Albuquerque",    "Tucson",    "Fresno",           "Sacramento",   "Mesa",
    "Kansas City",   "Atlanta",        "Omaha",     "Colorado Springs", "Raleigh",      "Miami",
    "Long Beach",    "Virginia Beach", "Oakland",   "Minneapolis",      "Tulsa",        "Arlington",
    "Wichita"};

struct ClientInfo {
	std::string nickname;
	std::string city;
	int run_count;
	bool initialize;
};

std::string GetRandomElement(const std::vector<std::string> &list) {
	static std::mt19937 gen(std::random_device {}());
	std::uniform_int_distribution<> dist(0, list.size() - 1);
	return list[dist(gen)];
}

ClientInfo LoadOrGenerateClientInfo(const std::string &filename) {
	ClientInfo info;
	std::ifstream file(filename);
	if (file.good()) {
		file >> info.nickname >> info.city >> info.run_count >> info.initialize;
		return info;
	}

	info.nickname = "user_" + std::to_string(rand() % 1500000);
	info.city = GetRandomElement(cities);
	info.run_count = 0;
	info.initialize = false;

	std::ofstream out(filename);
	out << info.nickname << " " << info.city << " " << info.run_count << " " << info.initialize;

	return info;
}

void SaveClientInfo(const std::string &filename, const ClientInfo &info) {
	std::ofstream out(filename);
	out << info.nickname << " " << info.city << " " << info.run_count << " " << true;
}

std::string FormatDate(int offset_days) {
	time_t now = time(0) + offset_days * 86400;
	tm *ltm = localtime(&now);
	char buf[11];
	strftime(buf, sizeof(buf), "%Y-%m-%d", ltm);
	return std::string(buf);
}

std::string FormatTime() {
	std::mt19937 gen(std::random_device {}());
	std::uniform_int_distribution<> h(5, 8), m(0, 59), s(0, 59);
	char buf[9];
	snprintf(buf, sizeof(buf), "%02d:%02d:%02d", h(gen), m(gen), s(gen));
	return std::string(buf);
}

void GenerateCSV(const std::string &filename, const ClientInfo &info) {
	std::string date = FormatDate(info.run_count);

	std::ofstream out(filename, std::ios::app);
	std::mt19937 gen(std::random_device {}());
	std::uniform_int_distribution<> row_count_dist(1, 5);
	int row_count = row_count_dist(gen);

	for (int i = 0; i < row_count; ++i) {
		std::string start_time = FormatTime();
		std::string end_time = FormatTime();
		int steps = 500 + (rand() % 10000);
		int heartbeat = 60 + (rand() % 80);

		out << info.nickname << "," << info.city << "," << date << "," << start_time << "," << end_time << "," << steps
		    << "," << heartbeat << "\n";
	}

	std::cout << "Generated " << row_count << " row(s) for date " << date << "\n";
}

void StoreTestData(ClientContext &context, const FunctionParameters &parameters) {
	const std::string info_file = "client_info.csv";
	ClientInfo info = LoadOrGenerateClientInfo(info_file);

	std::string filename = "test_data.csv";
	GenerateCSV(filename, info);

	if (!info.initialize) {
		DuckDB db(nullptr);
		Connection con(db);
		auto r = con.Query("pragma initialize_client");
		if (r->HasError()) {
			std::cerr << "Error while initializing client: " << r->GetError() << "\n";
		} else {
			std::cout << "Client initialized successfully.\n";
		}
	}

	DuckDB runs_db("runs.db");
	Connection runs_con(runs_db);

	auto r = runs_con.Query("insert into runs select * from read_csv_auto('" + filename + "');");
	if (r->HasError()) {
		std::cerr << "Error while inserting data: " << r->GetError() << "\n";
	} else {
		std::cout << "Data inserted successfully.\n";
	}

	info.run_count += 1;
	SaveClientInfo(info_file, info);

	r = runs_con.Query("pragma refresh('daily_runs_city');");
	if (r->HasError()) {
		std::cerr << "Error while refreshing materialized view: " << r->GetError() << "\n";
	} else {
		std::cout << "Materialized view refreshed successfully.\n";
	}

	r = runs_con.Query("delete from runs");
	if (r->HasError()) {
		std::cerr << "Error while deleting data: " << r->GetError() << "\n";
	}
	r = runs_con.Query("delete from daily_runs_city");
	if (r->HasError()) {
		std::cerr << "Error while deleting data: " << r->GetError() << "\n";
	} else {
		std::cout << "Data deleted successfully.\n";
	}
}

} // namespace duckdb
