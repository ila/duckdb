#include "storage/postgres_insert.hpp"
#include "storage/postgres_catalog.hpp"
#include "storage/postgres_transaction.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "storage/postgres_table_entry.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "postgres_connection.hpp"
#include "postgres_scanner.hpp"

namespace duckdb {

PostgresInsert::PostgresInsert(LogicalOperator &op, TableCatalogEntry &table,
                               physical_index_vector_t<idx_t> column_index_map_p)
    : PhysicalOperator(PhysicalOperatorType::EXTENSION, op.types, 1), table(&table), schema(nullptr),
      column_index_map(std::move(column_index_map_p)) {
}

PostgresInsert::PostgresInsert(LogicalOperator &op, SchemaCatalogEntry &schema, unique_ptr<BoundCreateTableInfo> info)
    : PhysicalOperator(PhysicalOperatorType::EXTENSION, op.types, 1), table(nullptr), schema(&schema),
      info(std::move(info)) {
}

//===--------------------------------------------------------------------===//
// States
//===--------------------------------------------------------------------===//
class PostgresInsertGlobalState : public GlobalSinkState {
public:
	explicit PostgresInsertGlobalState(ClientContext &context, PostgresTableEntry *table)
	    : table(table), insert_count(0) {
	}

	PostgresTableEntry *table;
	PostgresCopyState copy_state;
	DataChunk varchar_chunk;
	idx_t insert_count;
};

vector<string> GetInsertColumns(const PostgresInsert &insert, PostgresTableEntry &entry) {
	vector<string> column_names;
	auto &columns = entry.GetColumns();
	idx_t column_count;
	if (!insert.column_index_map.empty()) {
		column_count = 0;
		vector<PhysicalIndex> column_indexes;
		column_indexes.resize(columns.LogicalColumnCount(), PhysicalIndex(DConstants::INVALID_INDEX));
		for (idx_t c = 0; c < insert.column_index_map.size(); c++) {
			auto column_index = PhysicalIndex(c);
			auto mapped_index = insert.column_index_map[column_index];
			if (mapped_index == DConstants::INVALID_INDEX) {
				// column not specified
				continue;
			}
			column_indexes[mapped_index] = column_index;
			column_count++;
		}
		for (idx_t c = 0; c < column_count; c++) {
			auto &col = columns.GetColumn(column_indexes[c]);
			column_names.push_back(col.GetName());
		}
	}
	return column_names;
}

unique_ptr<GlobalSinkState> PostgresInsert::GetGlobalSinkState(ClientContext &context) const {
	PostgresTableEntry *insert_table;
	if (!table) {
		auto &schema_ref = *schema.get_mutable();
		insert_table =
		    &schema_ref.CreateTable(schema_ref.GetCatalogTransaction(context), *info)->Cast<PostgresTableEntry>();
	} else {
		insert_table = &table.get_mutable()->Cast<PostgresTableEntry>();
	}
	auto &transaction = PostgresTransaction::Get(context, insert_table->catalog);
	auto &connection = transaction.GetConnection();
	auto insert_columns = GetInsertColumns(*this, *insert_table);
	auto result = make_uniq<PostgresInsertGlobalState>(context, insert_table);
	auto format = insert_table->GetCopyFormat(context);
	vector<string> insert_column_names;
	if (!insert_columns.empty()) {
		for (auto &str : insert_columns) {
			auto index = insert_table->GetColumnIndex(str, true);
			if (!index.IsValid()) {
				insert_column_names.push_back(str);
			} else {
				insert_column_names.push_back(insert_table->postgres_names[index.index]);
			}
		}
	}
	connection.BeginCopyTo(context, result->copy_state, format, insert_table->schema.name, insert_table->name,
	                       insert_column_names);
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType PostgresInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = sink_state->Cast<PostgresInsertGlobalState>();
	auto &transaction = PostgresTransaction::Get(context.client, gstate.table->catalog);
	auto &connection = transaction.GetConnection();
	connection.CopyChunk(context.client, gstate.copy_state, chunk, gstate.varchar_chunk);
	gstate.insert_count += chunk.size();
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType PostgresInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                          OperatorSinkFinalizeInput &input) const {
	auto &gstate = sink_state->Cast<PostgresInsertGlobalState>();
	auto &transaction = PostgresTransaction::Get(context, gstate.table->catalog);
	auto &connection = transaction.GetConnection();
	connection.FinishCopyTo(gstate.copy_state);
	// update the approx_num_pages - approximately 8 bytes per column per row
	idx_t bytes_per_page = 8192;
	idx_t bytes_per_row = gstate.table->GetColumns().LogicalColumnCount() * 8;
	idx_t rows_per_page = MaxValue<idx_t>(1, bytes_per_page / bytes_per_row);
	gstate.table->approx_num_pages += gstate.insert_count / rows_per_page;
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// GetData
//===--------------------------------------------------------------------===//
SourceResultType PostgresInsert::GetData(ExecutionContext &context, DataChunk &chunk,
                                         OperatorSourceInput &input) const {
	auto &insert_gstate = sink_state->Cast<PostgresInsertGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(insert_gstate.insert_count));

	return SourceResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
string PostgresInsert::GetName() const {
	return table ? "INSERT" : "CREATE_TABLE_AS";
}

string PostgresInsert::ParamsToString() const {
	return table ? table->name : info->Base().table;
}

//===--------------------------------------------------------------------===//
// Plan
//===--------------------------------------------------------------------===//
unique_ptr<PhysicalOperator> AddCastToPostgresTypes(ClientContext &context, unique_ptr<PhysicalOperator> plan) {
	// check if we need to cast anything
	bool require_cast = false;
	auto &child_types = plan->GetTypes();
	for (auto &type : child_types) {
		auto postgres_type = PostgresUtils::ToPostgresType(type);
		if (postgres_type != type) {
			require_cast = true;
			break;
		}
	}
	if (require_cast) {
		vector<LogicalType> postgres_types;
		vector<unique_ptr<Expression>> select_list;
		for (idx_t i = 0; i < child_types.size(); i++) {
			auto &type = child_types[i];
			unique_ptr<Expression> expr;
			expr = make_uniq<BoundReferenceExpression>(type, i);

			auto postgres_type = PostgresUtils::ToPostgresType(type);
			if (postgres_type != type) {
				// add a cast
				expr = BoundCastExpression::AddCastToType(context, std::move(expr), postgres_type);
			}
			postgres_types.push_back(std::move(postgres_type));
			select_list.push_back(std::move(expr));
		}
		// we need to cast: add casts
		auto proj = make_uniq<PhysicalProjection>(std::move(postgres_types), std::move(select_list),
		                                          plan->estimated_cardinality);
		proj->children.push_back(std::move(plan));
		plan = std::move(proj);
	}

	return plan;
}

void PostgresCatalog::MaterializePostgresScans(PhysicalOperator &op) {
	if (op.type == PhysicalOperatorType::TABLE_SCAN) {
		auto &table_scan = op.Cast<PhysicalTableScan>();
		if (table_scan.function.name == "postgres_scan" || table_scan.function.name == "postgres_scan_pushdown" ||
		    table_scan.function.name == "postgres_query") {
			auto &bind_data = table_scan.bind_data->Cast<PostgresBindData>();
			bind_data.requires_materialization = true;
			bind_data.max_threads = 1;
			bind_data.emit_ctid = true;
		}
	}
	for (auto &child : op.children) {
		MaterializePostgresScans(*child);
	}
}

unique_ptr<PhysicalOperator> PostgresCatalog::PlanInsert(ClientContext &context, LogicalInsert &op,
                                                         unique_ptr<PhysicalOperator> plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not yet supported for insertion into Postgres table");
	}
	if (op.action_type != OnConflictAction::THROW) {
		throw BinderException("ON CONFLICT clause not yet supported for insertion into Postgres table");
	}
	MaterializePostgresScans(*plan);

	plan = AddCastToPostgresTypes(context, std::move(plan));

	auto insert = make_uniq<PostgresInsert>(op, op.table, op.column_index_map);
	insert->children.push_back(std::move(plan));
	return std::move(insert);
}

unique_ptr<PhysicalOperator> PostgresCatalog::PlanCreateTableAs(ClientContext &context, LogicalCreateTable &op,
                                                                unique_ptr<PhysicalOperator> plan) {
	plan = AddCastToPostgresTypes(context, std::move(plan));

	MaterializePostgresScans(*plan);

	auto insert = make_uniq<PostgresInsert>(op, op.schema, std::move(op.info));
	insert->children.push_back(std::move(plan));
	return std::move(insert);
}

} // namespace duckdb
