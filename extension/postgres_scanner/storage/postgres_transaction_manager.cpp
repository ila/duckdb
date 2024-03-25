#include "storage/postgres_transaction_manager.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

PostgresTransactionManager::PostgresTransactionManager(AttachedDatabase &db_p, PostgresCatalog &postgres_catalog)
    : TransactionManager(db_p), postgres_catalog(postgres_catalog) {
}

Transaction *PostgresTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_uniq<PostgresTransaction>(postgres_catalog, *this, context);
	transaction->Start();
	auto result = transaction.get();
	lock_guard<mutex> l(transaction_lock);
	transactions[result] = std::move(transaction);
	return result;
}

string PostgresTransactionManager::CommitTransaction(ClientContext &context, Transaction *transaction) {
	auto postgres_transaction = (PostgresTransaction *)transaction;
	postgres_transaction->Commit();
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
	return string();
}

void PostgresTransactionManager::RollbackTransaction(Transaction *transaction) {
	auto postgres_transaction = (PostgresTransaction *)transaction;
	postgres_transaction->Rollback();
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
}

void PostgresTransactionManager::Checkpoint(ClientContext &context, bool force) {
	auto &transaction = PostgresTransaction::Get(context, db.GetCatalog());
	auto &db = transaction.GetConnection();
	db.Execute("CHECKPOINT");
}

} // namespace duckdb
