#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/parser.hpp"
#include "postgres_connection.hpp"
#include "duckdb/common/types/uuid.hpp"

namespace duckdb {

static bool debug_postgres_print_queries = false;

PostgresConnection::PostgresConnection(shared_ptr<OwnedPostgresConnection> connection_p)
    : connection(std::move(connection_p)) {
}

PostgresConnection::~PostgresConnection() {
	Close();
}

PostgresConnection::PostgresConnection(PostgresConnection &&other) noexcept {
	std::swap(connection, other.connection);
	std::swap(dsn, other.dsn);
}

PostgresConnection &PostgresConnection::operator=(PostgresConnection &&other) noexcept {
	std::swap(connection, other.connection);
	std::swap(dsn, other.dsn);
	return *this;
}

PostgresConnection PostgresConnection::Open(const string &connection_string) {
	PostgresConnection result;
	result.connection = make_shared<OwnedPostgresConnection>(PostgresUtils::PGConnect(connection_string));
	result.dsn = connection_string;
	return result;
}

static bool ResultHasError(PGresult *result) {
	if (!result) {
		return true;
	}
	switch (PQresultStatus(result)) {
	case PGRES_COMMAND_OK:
	case PGRES_TUPLES_OK:
		return false;
	default:
		return true;
	}
}

PGresult *PostgresConnection::PQExecute(const string &query) {
	if (PostgresConnection::DebugPrintQueries()) {
		Printer::Print(query + "\n");
	}
	return PQexec(GetConn(), query.c_str());
}

unique_ptr<PostgresResult> PostgresConnection::TryQuery(const string &query, optional_ptr<string> error_message) {
	auto result = PQExecute(query.c_str());
	if (ResultHasError(result)) {
		if (error_message) {
			*error_message = StringUtil::Format("Failed to execute query \"" + query +
			                                    "\": " + string(PQresultErrorMessage(result)));
		}
		return nullptr;
	}
	return make_uniq<PostgresResult>(result);
}

unique_ptr<PostgresResult> PostgresConnection::Query(const string &query) {
	string error_msg;
	auto result = TryQuery(query, &error_msg);
	if (!result) {
		throw std::runtime_error(error_msg);
	}
	return result;
}

void PostgresConnection::Execute(const string &query) {
	Query(query);
}

vector<unique_ptr<PostgresResult>> PostgresConnection::ExecuteQueries(const string &queries) {
	if (PostgresConnection::DebugPrintQueries()) {
		Printer::Print(queries + "\n");
	}
	auto res = PQsendQuery(GetConn(), queries.c_str());
	if (res == 0) {
		throw std::runtime_error("Failed to execute query \"" + queries + "\": " + string(PQerrorMessage(GetConn())));
	}
	vector<unique_ptr<PostgresResult>> results;
	while (true) {
		auto res = PQgetResult(GetConn());
		if (!res) {
			break;
		}
		if (ResultHasError(res)) {
			throw std::runtime_error("Failed to execute query \"" + queries +
			                         "\": " + string(PQresultErrorMessage(res)));
		}
		if (PQresultStatus(res) != PGRES_TUPLES_OK) {
			continue;
		}
		auto result = make_uniq<PostgresResult>(res);
		results.push_back(std::move(result));
	}
	return results;
}

PostgresVersion PostgresConnection::GetPostgresVersion() {
	auto result =
	    Query("SELECT CURRENT_SETTING('server_version'), (SELECT COUNT(*) FROM pg_settings WHERE name LIKE 'rds%')");
	auto version = PostgresUtils::ExtractPostgresVersion(result->GetString(0, 0));
	if (result->GetInt64(0, 1) > 0) {
		version.type_v = PostgresInstanceType::AURORA;
	}
	return version;
}

bool PostgresConnection::IsOpen() {
	return connection.get();
}

void PostgresConnection::Close() {
	if (!IsOpen()) {
		return;
	}
	connection = nullptr;
}

vector<IndexInfo> PostgresConnection::GetIndexInfo(const string &table_name) {
	return vector<IndexInfo>();
}

void PostgresConnection::DebugSetPrintQueries(bool print) {
	debug_postgres_print_queries = print;
}

bool PostgresConnection::DebugPrintQueries() {
	return debug_postgres_print_queries;
}

} // namespace duckdb
