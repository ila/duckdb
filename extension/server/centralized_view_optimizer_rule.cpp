#include "include/centralized_view_optimizer_rule.hpp"

#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/common/printer.hpp>
#include <duckdb/parser/parser.hpp>
#include <duckdb/planner/planner.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include "duckdb/planner/binder.hpp"

#include <logical_plan_to_string.hpp>
#include <duckdb/function/aggregate/distributive_function_utils.hpp>
#include <duckdb/function/aggregate/distributive_functions.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/optimizer/optimizer.hpp>
#include <duckdb/planner/expression/bound_aggregate_expression.hpp>
#include <duckdb/planner/operator/logical_aggregate.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>

namespace duckdb {

void CVRewriteRule::CVRewriteRuleFunction(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	// first function call
	// the plan variable contains the plan for "SELECT * FROM flush('view_name');"
	if (plan->children.empty()) {
		return;
	}

	// check if plan contains table function 'flush'
	auto child = plan.get();
	while (!child->children.empty()) {
		child = child->children[0].get();
	}
	if (child->GetName().substr(0, 5) != "FLUSH") {
		return;
	}

	Printer::Print("Flushing...");
	auto get = dynamic_cast<LogicalGet *>(child);
	auto view = get->named_parameters["view_name"].ToString();
	// string query = "select c1, c2, c3, sum_steps from mv_test where c3 in (select c3 from (select c3, win,
	// count(distinct client_id) as clients from mv_test group by c3, win) where clients > 1)";
	string query = "select c3, win, count(distinct client_id) as clients from mv_test group by c3, win";
	Parser parser;
	Planner planner(input.context);

	parser.ParseQuery(query);
	auto statement = parser.statements[0].get();
	planner.CreatePlan(statement->Copy());
	planner.plan->Print();
	planner.plan->ResolveOperatorTypes();

	auto minimum_aggregation_column = "c3"; // todo also change

	// first of all we create two projection nodes on the same table
	auto table_catalog_entry = Catalog::GetEntry(input.context, CatalogType::TABLE_ENTRY, "test", "main", view,
	                                             OnEntryNotFound::THROW_EXCEPTION, QueryErrorContext());

	auto &table_entry = table_catalog_entry->Cast<TableCatalogEntry>();
	unique_ptr<FunctionData> bind_data;
	auto scan_function = table_entry.GetScanFunction(input.context, bind_data);
	vector<LogicalType> return_types = {};
	vector<string> return_names = {};
	vector<ColumnIndex> column_ids = {};

	// the left scan is always scanning 3 columns:
	// one with the minimum aggregation, the client id and the window
	// (for now we ignore the action, but this should be added later)
	// column_ids are the position of the column among all the columns
	// todo change the window column name

	for (size_t i = 0; i < table_entry.GetColumns().LogicalColumnCount(); i++) {
		if (table_entry.GetColumns().GetColumnNames()[i] == minimum_aggregation_column || table_entry.GetColumns().GetColumnNames()[i] == "win" || table_entry.GetColumns().GetColumnNames()[i] == "client_id") {
			// the size of column ids is the same as the names
			column_ids.push_back(ColumnIndex(i));
		}
		return_types.push_back(table_entry.GetColumns().GetColumnTypes()[i]);
		return_names.push_back(table_entry.GetColumns().GetColumnNames()[i]);
	}

	auto bind_data_copy = bind_data->Copy();
	auto return_types_copy(return_types);
	auto return_names_copy(return_names);

	auto left_get_index = input.optimizer.binder.GenerateTableIndex();
	auto left_get =
	    make_uniq<LogicalGet>(left_get_index, scan_function, move(bind_data), move(return_types), move(return_names));
	left_get->SetColumnIds(move(column_ids));
	left_get->GenerateColumnBindings(left_get_index, left_get->GetColumnIds().size());
	left_get->ResolveOperatorTypes();

	auto right_get_index = input.optimizer.binder.GenerateTableIndex();
	auto right_get = make_uniq<LogicalGet>(right_get_index, scan_function, std::move(bind_data_copy),
	                                       std::move(return_types_copy), std::move(return_names_copy));

	// now we create an aggregate node
	auto group_index = input.optimizer.binder.GenerateTableIndex();
	auto aggregate_index = input.optimizer.binder.GenerateTableIndex();
	auto groupings_index = input.optimizer.binder.GenerateTableIndex();
	vector<unique_ptr<Expression>> group_expressions;
	vector<unique_ptr<Expression>> aggregate_expressions; // groups
	// we are grouping by window and the minimum aggregation column
	int column_counter = 0; // auto-incrementing index to map the scan columns
	for (size_t i = 0; i < left_get->names.size(); i++) {
		if (left_get->names[i] == "win" || left_get->names[i] == minimum_aggregation_column) {
			group_expressions.emplace_back(make_uniq<BoundColumnRefExpression>(
			    left_get->names[i], left_get->returned_types[i], ColumnBinding(left_get_index, column_counter)));
			column_counter++;
		} else if (left_get->names[i] == "client_id") {
			// we also add the client id to the group by
			// this logic assumes the client_id to be the last column
			aggregate_expressions.emplace_back(make_uniq<BoundColumnRefExpression>(
			    left_get->names[i], left_get->returned_types[i], ColumnBinding(left_get_index, column_counter)));
			column_counter++;
		}
	}
	auto aggregate = make_uniq<LogicalAggregate>(group_index, aggregate_index, move(group_expressions));
	aggregate->groups = move(aggregate->expressions);
	aggregate->groupings_index = groupings_index;
	aggregate->ResolveOperatorTypes();
	// planner.plan->children[0]->children[0]->children[1]->children[0]->children[0]->children[0]

	// column bindings have the new indexes
	vector<LogicalType> logical_types;
	logical_types.emplace_back(LogicalTypeId::ANY);
	auto count_function = CountFunctionBase::GetFunction();
	auto bound_aggregate_expr = make_uniq<BoundAggregateExpression>(count_function, move(aggregate_expressions), nullptr,
	                                                                nullptr, AggregateType::DISTINCT);
	aggregate->expressions.push_back(move(bound_aggregate_expr));

	auto left_bottom_projection_index = input.optimizer.binder.GenerateTableIndex();
	vector<unique_ptr<Expression>> left_bottom_projection_expressions;
	for (size_t i = 0; i < aggregate->GetColumnBindings().size(); i++) {
		string name;
		LogicalType type;
		if (i < aggregate->groups.size()) {
			name = aggregate->groups[i]->GetAlias();
			type = aggregate->groups[i]->return_type;
		} else if (i < aggregate->groups.size() + aggregate->expressions.size()) {
			name = "clients";
			type = aggregate->expressions[i - aggregate->groups.size()]->return_type;
		}
		left_bottom_projection_expressions.emplace_back(make_uniq<BoundColumnRefExpression>(name, type,
		    ColumnBinding(aggregate->GetColumnBindings()[i].table_index, aggregate->GetColumnBindings()[i].column_index)));
	}

	// expressions are the columns from the group by
	auto left_bottom_projection = make_uniq<LogicalProjection>(left_bottom_projection_index, move(left_bottom_projection_expressions));
	left_bottom_projection->GenerateColumnBindings(left_bottom_projection_index, left_bottom_projection->expressions.size());

	aggregate->children.push_back(move(left_get));
	left_bottom_projection->children.push_back(move(aggregate));
	left_bottom_projection->Print();
	left_bottom_projection->ResolveOperatorTypes();
	left_bottom_projection->Verify(input.context);

	plan = move(left_bottom_projection);
	Printer::Print(LogicalPlanToString(input.context, plan));

}
} // namespace duckdb
