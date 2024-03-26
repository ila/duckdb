#include "storage/postgres_connection_pool.hpp"
#include "storage/postgres_catalog.hpp"

namespace duckdb {

PostgresPoolConnection::PostgresPoolConnection() : pool(nullptr) {
}

PostgresPoolConnection::PostgresPoolConnection(optional_ptr<PostgresConnectionPool> pool,
                                               PostgresConnection connection_p)
    : pool(pool), connection(std::move(connection_p)) {
}

PostgresPoolConnection::~PostgresPoolConnection() {
	if (pool) {
		pool->ReturnConnection(std::move(connection));
	}
}

PostgresPoolConnection::PostgresPoolConnection(PostgresPoolConnection &&other) noexcept {
	std::swap(pool, other.pool);
	std::swap(connection, other.connection);
}

PostgresPoolConnection &PostgresPoolConnection::operator=(PostgresPoolConnection &&other) noexcept {
	std::swap(pool, other.pool);
	std::swap(connection, other.connection);
	return *this;
}

bool PostgresPoolConnection::HasConnection() {
	return pool;
}

PostgresConnection &PostgresPoolConnection::GetConnection() {
	if (!HasConnection()) {
		throw InternalException("PostgresPoolConnection::GetConnection called without a transaction pool");
	}
	return connection;
}

PostgresConnectionPool::PostgresConnectionPool(PostgresCatalog &postgres_catalog, idx_t maximum_connections_p)
    : postgres_catalog(postgres_catalog), active_connections(0), maximum_connections(maximum_connections_p) {
}

bool PostgresConnectionPool::TryGetConnection(PostgresPoolConnection &connection) {
	lock_guard<mutex> l(connection_lock);
	if (active_connections >= maximum_connections) {
		return false;
	}
	active_connections++;
	// check if we have any cached connections left
	if (!connection_cache.empty()) {
		connection = PostgresPoolConnection(this, std::move(connection_cache.back()));
		connection_cache.pop_back();
		return true;
	}

	// no cached connections left but there is space to open a new one - open it
	connection = PostgresPoolConnection(this, PostgresConnection::Open(postgres_catalog.path));
	return true;
}

PostgresPoolConnection PostgresConnectionPool::GetConnection() {
	PostgresPoolConnection result;
	if (!TryGetConnection(result)) {
		throw IOException(
		    "Failed to get connection from PostgresConnectionPool - maximum connection count exceeded (%llu/%llu max)",
		    active_connections, maximum_connections);
	}
	return result;
}

void PostgresConnectionPool::ReturnConnection(PostgresConnection connection) {
	lock_guard<mutex> l(connection_lock);
	if (active_connections <= 0) {
		throw InternalException("PostgresConnectionPool::ReturnConnection called but active_connections is 0");
	}
	active_connections--;
	if (active_connections >= maximum_connections) {
		// if the maximum number of connections has been decreased by the user we might need to reclaim the connection
		// immediately
		return;
	}
	// check if the underlying connection is still usable
	auto pg_con = connection.GetConn();
	if (PQstatus(connection.GetConn()) != CONNECTION_OK) {
		// CONNECTION_BAD! try to reset it
		PQreset(pg_con);
		if (PQstatus(connection.GetConn()) != CONNECTION_OK) {
			// still bad - just abandon this one
			return;
		}
	}
	if (PQtransactionStatus(pg_con) != PQTRANS_IDLE) {
		return;
	}
	connection_cache.push_back(std::move(connection));
}

void PostgresConnectionPool::SetMaximumConnections(idx_t new_max) {
	lock_guard<mutex> l(connection_lock);
	if (new_max < maximum_connections) {
		// potentially close connections
		// note that we can only close connections in the connection cache
		// we will have to wait for connections to be returned
		auto total_open_connections = active_connections + connection_cache.size();
		while (!connection_cache.empty() && total_open_connections > new_max) {
			total_open_connections--;
			connection_cache.pop_back();
		}
	}
	maximum_connections = new_max;
}

} // namespace duckdb
