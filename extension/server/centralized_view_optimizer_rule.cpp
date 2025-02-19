#include "include/centralized_view_optimizer_rule.hpp"

#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/common/printer.hpp>
#include <duckdb/parser/parser.hpp>
#include <duckdb/planner/planner.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include "duckdb/planner/binder.hpp"

#include <logical_plan_to_string.hpp>
#include <regex>
#include <duckdb/function/aggregate/distributive_function_utils.hpp>
#include <duckdb/function/aggregate/distributive_functions.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/optimizer/optimizer.hpp>
#include <duckdb/planner/expression/bound_aggregate_expression.hpp>
#include <duckdb/planner/operator/logical_aggregate.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>
#include <duckdb/planner/operator/logical_filter.hpp>
#include <duckdb/planner/operator/logical_comparison_join.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>


namespace duckdb {

void CVRewriteRule::CVRewriteRuleFunction(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	// the plan variable contains the plan for "SELECT * FROM flush('view_name');"

	// for flush, we need to:
	// 1. insert into the centralized table the columns meeting min agg
	// 2. removing the set from 1) in the materialized view
	// 3. also remove everything with expired TTL

	/* this function inserts in the centralized view the result of a query in the form of
	 *	select c1, c2, c3, sum_steps
	 *	from mv_test
	 *	where c3 in
	 *		(select c3 from
	 *			(select c3, win, count(distinct client_id) as clients
	 *			from mv_test group by c3, win)
	 *		where clients > 1);
	 * in other words, we check that the minimum aggregation is respected, and if so, we flush the tuples
	 * here, we generate a plan completely from scratch, using a few assumptions about our metadata
	 * also, we create the non-optimized plan, and then we optimize it
	 *
	 * potential issues might arise from the ordering of the columns, or the extraction of indexes
	 * in the aggregate and join logic
	 */

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

	auto child_get = dynamic_cast<LogicalGet *>(child);

	auto view = child_get->named_parameters["view_name"].ToString();
	auto minimum_aggregation_column = child_get->named_parameters["min_agg_col_name"].ToString();
	auto minimum_aggregation_value = std::stoi(child_get->named_parameters["min_agg_value"].ToString());
	LogicalType minimum_aggregation_type = LogicalType::VARCHAR; // todo

	// first of all we create two projection nodes on the same table
	auto table_catalog_entry = Catalog::GetEntry(input.context, CatalogType::TABLE_ENTRY, "test", "main", view,
	                                             OnEntryNotFound::THROW_EXCEPTION, QueryErrorContext());

	auto &table_entry = table_catalog_entry->Cast<TableCatalogEntry>();
	unique_ptr<FunctionData> bind_data;
	auto scan_function = table_entry.GetScanFunction(input.context, bind_data);
	vector<LogicalType> return_types_right = {};
	vector<string> return_names_right = {};
	vector<ColumnIndex> column_ids_right = {};

	// the right scan is always scanning 3 columns:
	// one with the minimum aggregation, the client id and the window
	// (for now we ignore the action, but this should be added later)
	// column_ids are the position of the column among all the columns
	// todo change the window column name

	for (size_t i = 0; i < table_entry.GetColumns().LogicalColumnCount(); i++) {
		if (table_entry.GetColumns().GetColumnNames()[i] == minimum_aggregation_column || table_entry.GetColumns().GetColumnNames()[i] == "rdda_window" || table_entry.GetColumns().GetColumnNames()[i] == "client_id") {
			// the size of column ids is the same as the names
			column_ids_right.emplace_back(ColumnIndex(i));
			if (table_entry.GetColumns().GetColumnNames()[i] == minimum_aggregation_column) {
				minimum_aggregation_type = table_entry.GetColumns().GetColumnTypes()[i];
			}
		}
		return_types_right.emplace_back(table_entry.GetColumns().GetColumnTypes()[i]);
		return_names_right.emplace_back(table_entry.GetColumns().GetColumnNames()[i]);
	}

	auto bind_data_left = bind_data->Copy();

	auto right_get_index = input.optimizer.binder.GenerateTableIndex();
	auto right_get =
	    make_uniq<LogicalGet>(right_get_index, scan_function, move(bind_data), move(return_types_right), move(return_names_right));
	right_get->SetColumnIds(move(column_ids_right));
	right_get->GenerateColumnBindings(right_get_index, right_get->GetColumnIds().size());
	right_get->ResolveOperatorTypes();

	// now we create an aggregate node
	auto group_index = input.optimizer.binder.GenerateTableIndex();
	auto aggregate_index = input.optimizer.binder.GenerateTableIndex();
	auto groupings_index = input.optimizer.binder.GenerateTableIndex();
	vector<unique_ptr<Expression>> group_expressions;
	vector<unique_ptr<Expression>> aggregate_expressions; // groups
	// we are grouping by window and the minimum aggregation column
	int column_counter = 0; // auto-incrementing index to map the scan columns
	for (size_t i = 0; i < right_get->names.size(); i++) {
		if (right_get->names[i] == "rdda_window" || right_get->names[i] == minimum_aggregation_column) {
			group_expressions.emplace_back(make_uniq<BoundColumnRefExpression>(
			    right_get->names[i], right_get->returned_types[i], ColumnBinding(right_get_index, column_counter)));
			column_counter++;
		} else if (right_get->names[i] == "client_id") {
			// we also add the client id to the group by
			// this logic assumes the client_id to be the last column
			aggregate_expressions.emplace_back(make_uniq<BoundColumnRefExpression>(
			    right_get->names[i], right_get->returned_types[i], ColumnBinding(right_get_index, column_counter)));
			column_counter++;
		}
	}
	auto aggregate = make_uniq<LogicalAggregate>(group_index, aggregate_index, move(group_expressions));
	aggregate->groups = move(aggregate->expressions);
	aggregate->groupings_index = groupings_index;
	aggregate->ResolveOperatorTypes();

	// column bindings have the new indexes
	vector<LogicalType> logical_types;
	logical_types.emplace_back(LogicalTypeId::ANY);
	auto count_function = CountFunctionBase::GetFunction();
	auto bound_aggregate_expr = make_uniq<BoundAggregateExpression>(count_function, move(aggregate_expressions), nullptr,
	                                                                nullptr, AggregateType::DISTINCT);
	// auto bound_aggregate_expr = make_uniq<BoundAggregateExpression>(count_function, move(aggregate_expressions), nullptr,
	// 															nullptr, AggregateType::NON_DISTINCT);
	aggregate->expressions.emplace_back(move(bound_aggregate_expr));

	auto right_bottom_projection_index = input.optimizer.binder.GenerateTableIndex();
	vector<unique_ptr<Expression>> right_bottom_projection_expressions;
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
		right_bottom_projection_expressions.emplace_back(make_uniq<BoundColumnRefExpression>(name, type,
		    ColumnBinding(aggregate->GetColumnBindings()[i].table_index, aggregate->GetColumnBindings()[i].column_index)));
	}

	// expressions are the columns from the group by
	auto right_bottom_projection = make_uniq<LogicalProjection>(right_bottom_projection_index, move(right_bottom_projection_expressions));
	right_bottom_projection->GenerateColumnBindings(right_bottom_projection_index, right_bottom_projection->expressions.size());

	aggregate->children.emplace_back(move(right_get));
	right_bottom_projection->children.emplace_back(move(aggregate));
	right_bottom_projection->ResolveOperatorTypes();

	size_t filter_binding_table = 0;
	size_t filter_binding_column = 0;
	LogicalType filter_type;

	// now we create the top right projection, only taking the minimum aggregation column
	// in the meantime we also take the column bindings of the clients column for the where clause
	auto right_top_projection_index = input.optimizer.binder.GenerateTableIndex();
	vector<unique_ptr<Expression>> right_top_projection_expressions;
	// technically this is not needed if the minimum aggregation column is first
	for (size_t i = 0; i < right_bottom_projection->GetColumnBindings().size(); i++) {
		if (right_bottom_projection->expressions[i]->alias == minimum_aggregation_column) {
			right_top_projection_expressions.emplace_back(make_uniq<BoundColumnRefExpression>(minimum_aggregation_column, right_bottom_projection->types[i], ColumnBinding(right_bottom_projection->GetColumnBindings()[i].table_index, right_bottom_projection->GetColumnBindings()[i].column_index)));
		} else if (right_bottom_projection->expressions[i]->alias == "clients") {
			filter_binding_column = right_bottom_projection->GetColumnBindings()[i].column_index;
			filter_binding_table = right_bottom_projection->GetColumnBindings()[i].table_index;
			filter_type = right_bottom_projection->types[i];
		}
	}

	auto right_filter_column_expression = make_uniq<BoundColumnRefExpression>("clients", filter_type, ColumnBinding(filter_binding_table, filter_binding_column));
	auto minimum_aggregation_value_expression = make_uniq<BoundConstantExpression>(Value::BIGINT(minimum_aggregation_value));
	auto right_filter_expression = make_uniq<BoundComparisonExpression>(
				ExpressionType::COMPARE_GREATERTHANOREQUALTO,
				move(right_filter_column_expression),
				move(minimum_aggregation_value_expression)
				);

	auto right_filter = make_uniq<LogicalFilter>(move(right_filter_expression));
	for (auto &type : right_bottom_projection->types) {
		right_filter->types.emplace_back(type);
	}

	right_filter->children.emplace_back(move(right_bottom_projection));
	right_filter->ResolveOperatorTypes(); // this should not do anything, check the types of the child

	auto right_top_projection = make_uniq<LogicalProjection>(right_top_projection_index, move(right_top_projection_expressions));
	right_top_projection->GenerateColumnBindings(right_top_projection_index, right_top_projection->expressions.size());
	right_top_projection->children.emplace_back(move(right_filter));
	right_top_projection->ResolveOperatorTypes();
	right_top_projection->Verify(input.context);

	// now on to the left side
	vector<LogicalType> return_types_left = {};
	vector<string> return_names_left = {};
	vector<ColumnIndex> column_ids_left = {};

	size_t join_column_position;
	vector<string> names;

	// for the left scan we need all the columns except the metadata
	for (size_t i = 0; i < table_entry.GetColumns().LogicalColumnCount(); i++) {
		if (table_entry.GetColumns().GetColumnNames()[i] != "action" && table_entry.GetColumns().GetColumnNames()[i] != "rdda_window" && table_entry.GetColumns().GetColumnNames()[i] != "client_id"
			&& table_entry.GetColumns().GetColumnNames()[i] != "generation" && table_entry.GetColumns().GetColumnNames()[i] != "arrival") {
			// the size of column ids is the same as the names
			column_ids_left.emplace_back(ColumnIndex(i));
			names.emplace_back(table_entry.GetColumns().GetColumnNames()[i]);
			if (table_entry.GetColumns().GetColumnNames()[i] == minimum_aggregation_column) {
				join_column_position = i;
			}
		}
		return_types_left.emplace_back(table_entry.GetColumns().GetColumnTypes()[i]);
		return_names_left.emplace_back(table_entry.GetColumns().GetColumnNames()[i]);
	}

	auto left_get_index = input.optimizer.binder.GenerateTableIndex();
	auto left_get = make_uniq<LogicalGet>(left_get_index, scan_function, std::move(bind_data_left),
										   std::move(return_types_left), std::move(return_names_left));
	left_get->SetColumnIds(move(column_ids_left));
	left_get->GenerateColumnBindings(left_get_index, left_get->GetColumnIds().size());
	left_get->ResolveOperatorTypes();

	// now the join
	// todo - I am not sure this logic for the join column position is correct - test more
	auto join_condition = make_uniq<BoundComparisonExpression>(
		ExpressionType::COMPARE_EQUAL,
		make_uniq<BoundColumnRefExpression>(minimum_aggregation_column, minimum_aggregation_type, ColumnBinding(left_get_index, join_column_position)),
		make_uniq<BoundColumnRefExpression>(minimum_aggregation_column, minimum_aggregation_type, ColumnBinding(right_top_projection_index, 0))
	);
	auto join = LogicalComparisonJoin::CreateJoin(input.context, JoinType::MARK, JoinRefType::REGULAR, move(left_get), move(right_top_projection), move(join_condition));
	join->GetColumnBindings(); // this automatically generates left and right children bindings
	join->ResolveOperatorTypes();

	auto top_projection_index = input.optimizer.binder.GenerateTableIndex();
	vector<unique_ptr<Expression>> top_projection_expressions;
	for (size_t i = 0; i < join->GetColumnBindings().size() - 1; i++) {
		top_projection_expressions.emplace_back(make_uniq<BoundColumnRefExpression>(names[i], join->types[i],
			ColumnBinding(join->GetColumnBindings()[i].table_index, join->GetColumnBindings()[i].column_index)));
	}

	// todo check the scan data pointer
	auto top_projection = make_uniq<LogicalProjection>(top_projection_index, move(top_projection_expressions));
	top_projection->GenerateColumnBindings(top_projection_index, top_projection->expressions.size());

	top_projection->children.emplace_back(move(join));
	top_projection->ResolveOperatorTypes();
	top_projection->Verify(input.context);

	// replace "centralized_view" with "centralized_table"
	auto table = std::regex_replace(view, std::regex("centralized_view"), "centralized_table");
	auto centralized_table_catalog_entry = Catalog::GetEntry(input.context, CatalogType::TABLE_ENTRY, "test", "main", table,
											 OnEntryNotFound::THROW_EXCEPTION, QueryErrorContext());

	plan = move(top_projection);
	plan->Print();

	// auto &centralized_table_entry = centralized_table_catalog_entry->Cast<TableCatalogEntry>();
	//
	// auto insert_index = input.optimizer.binder.GenerateTableIndex();
	// auto insert = make_uniq<LogicalInsert>(centralized_table_entry, insert_index);
	//
	// // generate bindings for the insert node using the top node of the plan
	// Value value;
	// unique_ptr<BoundConstantExpression> exp;
	// for (size_t i = 0; i < plan->expressions.size(); i++) {
	// 	insert->expected_types.emplace_back(plan->expressions[i]->return_type);
	// 	value = Value(plan->expressions[i]->return_type);
	// 	exp = make_uniq<BoundConstantExpression>(move(value));
	// 	insert->bound_defaults.emplace_back(move(exp));
	// }
	//
	// insert->children.emplace_back(move(top_projection));
	// insert->Print();
	//
	// plan = move(insert);
}
} // namespace duckdb
