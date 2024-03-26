#include "storage/postgres_delete.hpp"
#include "storage/postgres_table_entry.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "storage/postgres_catalog.hpp"
#include "storage/postgres_transaction.hpp"
#include "postgres_connection.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

PostgresDelete::PostgresDelete(LogicalOperator &op, TableCatalogEntry &table, idx_t row_id_index)
    : PhysicalOperator(PhysicalOperatorType::EXTENSION, op.types, 1), table(table), row_id_index(row_id_index) {
}

//===--------------------------------------------------------------------===//
// States
//===--------------------------------------------------------------------===//
string GetDeleteSQL(const string &table_name, const string &ctid_list) {
	string result;
	result = "DELETE FROM " + KeywordHelper::WriteOptionallyQuoted(table_name);
	result += " WHERE ctid IN (" + ctid_list + ")";
	return result;
}

class PostgresDeleteGlobalState : public GlobalSinkState {
public:
	explicit PostgresDeleteGlobalState(PostgresTableEntry &table) : table(table), delete_count(0) {
	}

	PostgresTableEntry &table;
	string ctid_list;
	idx_t delete_count;

	void Flush(ClientContext &context) {
		if (ctid_list.empty()) {
			return;
		}
		auto &transaction = PostgresTransaction::Get(context, table.catalog);
		transaction.Query(GetDeleteSQL(table.name, ctid_list));
		ctid_list = "";
	}
};

unique_ptr<GlobalSinkState> PostgresDelete::GetGlobalSinkState(ClientContext &context) const {
	auto &postgres_table = table.Cast<PostgresTableEntry>();

	auto &transaction = PostgresTransaction::Get(context, postgres_table.catalog);
	auto result = make_uniq<PostgresDeleteGlobalState>(postgres_table);
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType PostgresDelete::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<PostgresDeleteGlobalState>();

	chunk.Flatten();
	auto &row_identifiers = chunk.data[row_id_index];
	auto row_data = FlatVector::GetData<row_t>(row_identifiers);
	for (idx_t i = 0; i < chunk.size(); i++) {
		if (!gstate.ctid_list.empty()) {
			gstate.ctid_list += ",";
		}
		// extract the ctid from the row id
		auto row_in_page = row_data[i] & 0xFFFF;
		auto page_index = row_data[i] >> 16;
		gstate.ctid_list += "'(";
		gstate.ctid_list += to_string(page_index);
		gstate.ctid_list += ",";
		gstate.ctid_list += to_string(row_in_page);
		gstate.ctid_list += ")'";
		if (gstate.ctid_list.size() > 3000) {
			// avoid making too long SQL statements
			gstate.Flush(context.client);
		}
	}
	gstate.delete_count += chunk.size();
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType PostgresDelete::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                          OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<PostgresDeleteGlobalState>();
	gstate.Flush(context);
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// GetData
//===--------------------------------------------------------------------===//
SourceResultType PostgresDelete::GetData(ExecutionContext &context, DataChunk &chunk,
                                         OperatorSourceInput &input) const {
	auto &insert_gstate = sink_state->Cast<PostgresDeleteGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(insert_gstate.delete_count));

	return SourceResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
string PostgresDelete::GetName() const {
	return "DELETE";
}

string PostgresDelete::ParamsToString() const {
	return table.name;
}

//===--------------------------------------------------------------------===//
// Plan
//===--------------------------------------------------------------------===//
unique_ptr<PhysicalOperator> PostgresCatalog::PlanDelete(ClientContext &context, LogicalDelete &op,
                                                         unique_ptr<PhysicalOperator> plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not yet supported for deletion of a Postgres table");
	}
	auto &bound_ref = op.expressions[0]->Cast<BoundReferenceExpression>();
	PostgresCatalog::MaterializePostgresScans(*plan);

	auto insert = make_uniq<PostgresDelete>(op, op.table, bound_ref.index);
	insert->children.push_back(std::move(plan));
	return std::move(insert);
}

} // namespace duckdb
