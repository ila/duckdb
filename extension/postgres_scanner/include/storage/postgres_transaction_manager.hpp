//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/postgres_transaction_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction_manager.hpp"
#include "storage/postgres_catalog.hpp"
#include "storage/postgres_transaction.hpp"

namespace duckdb {

class PostgresTransactionManager : public TransactionManager {
public:
	PostgresTransactionManager(AttachedDatabase &db_p, PostgresCatalog &postgres_catalog);

	Transaction *StartTransaction(ClientContext &context) override;
	string CommitTransaction(ClientContext &context, Transaction *transaction) override;
	void RollbackTransaction(Transaction *transaction) override;

	void Checkpoint(ClientContext &context, bool force = false) override;

private:
	PostgresCatalog &postgres_catalog;
	mutex transaction_lock;
	unordered_map<Transaction *, unique_ptr<PostgresTransaction>> transactions;
};

} // namespace duckdb
