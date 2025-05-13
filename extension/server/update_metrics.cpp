//
// Created by ila on 4/28/25.
//

#include "update_metrics.hpp"

#include <common.hpp>

namespace duckdb {

string UpdateResponsiveness(string &view_name) {

	// responsiveness (per window) is defined as how many clients have sent data in a centralized table
	// in other words, for each window, we need to count how many clients have sent data
	// and divide it by the total number of clients

	string query = "attach 'rdda_parser.db' as rdda_parser (read_only);\n\n";

	query += "WITH distinct_clients_per_window AS (\n"
	         "\tSELECT rdda_window, COUNT(DISTINCT client_id) AS window_client_count\n"
	         "\tFROM rdda_centralized_table_" +
	         view_name +
	         "\n"
	         "\tGROUP BY rdda_window),\n"
	         "total_clients AS (\n"
	         "\tSELECT COUNT(DISTINCT id) AS total_client_count FROM rdda_parser.rdda_clients),\n"
	         "percentages AS (\n"
	         "\tSELECT d.rdda_window, (d.window_client_count::decimal / t.total_client_count) * 100 AS percentage\n"
	         "\tFROM distinct_clients_per_window d, total_clients t)\n"
	         "UPDATE rdda_centralized_table_" +
	         view_name +
	         " rdda_metadata_update\n"
	         "SET responsiveness = p.percentage\n"
	         "FROM percentages p\n"
	         "WHERE rdda_metadata_update.rdda_window = p.rdda_window;\n\n";

	query += "detach rdda_parser;\n\n";

	return query;
}

string UpdateCompleteness(string &view_name) {

	// completeness is defined as the percentage of tuples that have been discarded compared to the total
	string discard_query = ",\n"; // to join with the cte
	discard_query += "to_discard AS (\n"
	                       "\tSELECT rdda_window, COUNT(*) AS discarded_count\n"
	                       "\tFROM  rdda_client.rdda_centralized_view_" +
	                       view_name +
	                       "\n"
	                       "\tWHERE rdda_window <= (SELECT expired_window FROM threshold_window)\n"
	                       "\tGROUP BY rdda_window),\n"
	                       "to_keep AS (\n"
	                       "\tSELECT rdda_window, COUNT(*) AS kept_count\n"
	                       "\tFROM rdda_centralized_table_" +
	                       view_name +
	                       "\n"
	                       "\tGROUP BY rdda_window),\n"
	                       "combined AS (\n"
	                       "\tSELECT\n"
	                       "\t\tCOALESCE(d.rdda_window, k.rdda_window) AS rdda_window,\n"
	                       "\t\tCOALESCE(d.discarded_count, 0) AS discarded,\n"
	                       "\t\tCOALESCE(k.kept_count, 0) AS kept\n"
	                       "\tFROM to_discard d\n"
	                       "\tFULL OUTER JOIN to_keep k\n"
	                       "\tON d.rdda_window = k.rdda_window),\n"
	                       "discard_stats AS (\n"
	                       "\tSELECT rdda_window, discarded, kept, \n"
	                       "\t\tCASE WHEN (discarded + kept) > 0 THEN (kept::decimal / (discarded + kept)) * 100 "
	                       "ELSE 100 END AS discard_percentage\n"
	                       "\tFROM combined)\n"
	                       "UPDATE rdda_centralized_table_" +
	                       view_name +
	                       " rdda_metadata_update\n"
	                       "SET completeness = ds.discard_percentage\n"
	                       "FROM discard_stats ds\n"
	                       "WHERE rdda_metadata_update.rdda_window = rdda_metadata_update.rdda_window;\n\n";

	return discard_query;
}

string UpdateBufferSize(string &view_name) {

	string buffer_query = "detach rdda_parser;\n\n";
	buffer_query += "WITH buffer_counts AS (\n"
	                      "\tSELECT rdda_window, COUNT(*) AS buffer_count\n"
	                      "\tFROM  rdda_client.rdda_centralized_view_" +
	                      view_name +
	                      "\n"
	                      "\tGROUP BY rdda_window),\n"
	                      "centralized_counts AS (\n"
	                      "\tSELECT rdda_window, COUNT(*) AS centralized_count\n"
	                      "\tFROM rdda_centralized_table_" +
	                      view_name +
	                      "\n"
	                      "\tGROUP BY rdda_window),\n"
	                      "combined AS (\n"
	                      "\tSELECT\n"
	                      "\tCOALESCE(b.rdda_window, c.rdda_window) AS rdda_window,\n"
	                      "\tCOALESCE(b.buffer_count, 0) AS buffer,\n"
	                      "\tCOALESCE(c.centralized_count, 0) AS centralized\n"
	                      "\tFROM buffer_counts b\n"
	                      "\tFULL OUTER JOIN centralized_counts c\n"
	                      "\tON b.rdda_window = c.rdda_window),\n"
	                      "buffer_stats AS (\n"
	                      "\tSELECT rdda_window, buffer, centralized,\n"
	                      "\t\tCASE WHEN (buffer + centralized) > 0 THEN (buffer::decimal / (buffer + centralized)) * "
	                      "100 ELSE 0 END AS buffer_percentage\n"
	                      "\tFROM combined) "
	                      "UPDATE rdda_centralized_table_" +
	                      view_name +
	                      " rdda_metadata_update\n"
	                      "SET buffer_size = bs.buffer_percentage\n"
	                      "FROM buffer_stats bs\n"
	                      "WHERE rdda_metadata_update.rdda_window = bs.rdda_window;\n\n";

	return buffer_query;
}

string CleanupExpiredClients(std::unordered_map<string, string> &config) {

	int keep_alive_days = std::stoi(config["keep_alive_clients_days"]);
	string query = "delete from rdda_parser.rdda_clients where last_update < today() - interval " +
	         std::to_string(keep_alive_days) + " day;\n\n";
	// todo - do we need to delete anything else? refresh history?
	query += "detach rdda_parser;\n\n";
	return query;
}

} // namespace duckdb
