
#ifndef DUCKDB_OPENIVM_REWRITE_RULE_HPP
#define DUCKDB_OPENIVM_REWRITE_RULE_HPP

#include "../../compiler/include/logical_plan_to_string.hpp"
#include "../../postgres_scanner/include/postgres_scanner.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/planner/tableref/bound_basetableref.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "openivm_parser.hpp"

#include <iostream>
#include <utility>

namespace duckdb {

class IVMRewriteRule : public OptimizerExtension {
public:
	IVMRewriteRule() {
		optimize_function = IVMRewriteRuleFunction;
	}

	static void AddInsertNode(ClientContext &context, unique_ptr<LogicalOperator> &plan,
	                          string &view_name, string &view_catalog_name, string &view_schema_name) {
#ifdef DEBUG
		printf("\nAdd the insert node to the plan...\n");
		printf("Plan:\n%s\nParameters:", plan->ToString().c_str());
		// Get whatever ParameterToString yields.
		for (const auto& i_param : plan->ParamsToString()) {
			printf("%s", i_param.second.c_str());
		}
		printf("\n---end of insert node output---\n");
#endif

		auto delta_table_catalog_entry =
		    Catalog::GetEntry(context, CatalogType::TABLE_ENTRY, view_catalog_name, view_schema_name,
		                      "delta_" + view_name, OnEntryNotFound::RETURN_NULL, QueryErrorContext());
		optional_ptr<TableCatalogEntry> table = dynamic_cast<TableCatalogEntry *>(delta_table_catalog_entry.get());
		// create insert node. It is new node, hence it gets a new table_idx
		// putting an arbitrary index here
		// todo -- do we even need this insert node on top?
		auto insert_node = make_uniq<LogicalInsert>(*table, 999);

		// generate bindings for the insert node using the top node of the plan
		Value value;
		unique_ptr<BoundConstantExpression> exp;
		for (size_t i = 0; i < plan->expressions.size(); i++) {
			insert_node->expected_types.emplace_back(plan->expressions[i]->return_type);
			value = Value(plan->expressions[i]->return_type);
			exp = make_uniq<BoundConstantExpression>(std::move(value));
			insert_node->bound_defaults.emplace_back(std::move(exp));
		}

		// insert the insert node at the top of the plan
		insert_node->children.emplace_back(std::move(plan));
		plan = std::move(insert_node);
	}

	static void ModifyTopNode(ClientContext &context, unique_ptr<LogicalOperator> &plan, idx_t &multiplicity_col_idx,
	                          idx_t &multiplicity_table_idx) {

#ifdef DEBUG
		if (plan == nullptr) {
			printf("\nModifyTopNode: received nullptr as input!\n");
		}
#endif
		if (plan->type != LogicalOperatorType::LOGICAL_PROJECTION) {
			throw NotImplementedException("Assumption being made: top node has to be projection node");
		}

#ifdef DEBUG
		printf("\nAdd the multiplicity column to the top projection node...\n");
		printf("Plan:\n%s\nParameters:", plan->ToString().c_str());
		// Output ParameterToString.
		for (const auto& i_param : plan->ParamsToString()) {
			printf("%s", i_param.second.c_str());
		}
		for (size_t i = 0; i < plan->GetColumnBindings().size(); i++) {
			printf("\nTop node CB before %zu %s", i, plan->GetColumnBindings()[i].ToString().c_str());
		}
		printf("\n---end of ModifyTopNode (multiplicity column) output---\n");
#endif

		// the table_idx used to create ColumnBinding will be that of the top node's child
		// the column_idx used to create ColumnBinding for multiplicity column will be stored along with the context
		// from the child node
		if (plan->children[0]->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
			// if we have an aggregate, we can't extract the column index from the expression
			// the expression might be an aggregate and the multiplicity column will be a grouping column
			// example: with queries like "SELECT COUNT(*) FROM table", the binding will be 3, but we want 2
			multiplicity_table_idx = dynamic_cast<LogicalAggregate *>(plan->children[0].get())->group_index;
		} else {
			// this might break with joins
			multiplicity_table_idx = dynamic_cast<BoundColumnRefExpression *>(plan->expressions[0].get())->binding.table_index;
		}
		//multiplicity_col_idx = plan->GetColumnBindings().size();
		auto e = make_uniq<BoundColumnRefExpression>("_duckdb_ivm_multiplicity", LogicalType::BOOLEAN,
		                                             ColumnBinding(multiplicity_table_idx, multiplicity_col_idx));
		plan->expressions.emplace_back(std::move(e));

#ifdef DEBUG
		printf("Plan:\n%s\nParameters:", plan->ToString().c_str());
		// Output ParameterToString.
		for (const auto& i_param : plan->ParamsToString()) {
			printf("%s", i_param.second.c_str());
		}
		for (size_t i = 0; i < plan.get()->GetColumnBindings().size(); i++) {
			printf("Top node CB %zu %s\n", i, plan.get()->GetColumnBindings()[i].ToString().c_str());
		}
		printf("\n---end of ModifyTopNode (finish) output---\n");
#endif
	}

	static unique_ptr<LogicalOperator> ModifyPlan(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan,
	                       idx_t &multiplicity_col_idx, idx_t &multiplicity_table_idx,
	                       optional_ptr<CatalogEntry> &table_catalog_entry, string &view, string &table) {
		// previously: Assume only one child per node
		ClientContext &context = input.context;
		// now: Support modification of plan with multiple children.
		unique_ptr<LogicalOperator> left_child, right_child;
		if (plan.get()->type == LogicalOperatorType::LOGICAL_JOIN) {
			left_child =  plan->children[0]->Copy(context);
			right_child =  plan->children[1]->Copy(context);
		}
		for (auto &&child : plan->children) {
			child = ModifyPlan(input, child, multiplicity_col_idx, multiplicity_table_idx,
					   table_catalog_entry, view, table);
		}
		QueryErrorContext error_context = QueryErrorContext();

		switch (plan->type) {
		case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		case LogicalOperatorType::LOGICAL_JOIN: {
			auto join = static_cast<LogicalJoin*>(plan.get());
			if (join->join_type != JoinType::INNER) {
				throw Exception(ExceptionType::OPTIMIZER, JoinTypeToString(join->join_type) + " type not yet supported in OpenIVM");
			}
			auto left_delta = std::move(plan->children[0]);
			auto right_delta = std::move(plan->children[1]);
			auto join1 = plan->Copy(context);
			auto join2 = plan->Copy(context);
			auto join3 = plan->Copy(context);
			join1->children[0] = std::move(left_child);
			join2->children[1] = std::move(right_child);
			auto copy_union = make_uniq<LogicalSetOperation>(input.optimizer.binder.GenerateTableIndex(), 1U, std::move(join1),
															 std::move(join2), LogicalOperatorType::LOGICAL_UNION, true);
			plan = make_uniq<LogicalSetOperation>(input.optimizer.binder.GenerateTableIndex(), 1U, std::move(copy_union),
															 std::move(join3), LogicalOperatorType::LOGICAL_UNION, true);
			break; // TODO: Finish.
		}
		case LogicalOperatorType::LOGICAL_GET: {
			// we are at the bottom of the tree
			auto old_get = dynamic_cast<LogicalGet *>(plan.get());

#ifdef DEBUG
			printf("Create replacement get node \n");
#endif
			string delta_table;
			string delta_table_schema;
			string delta_table_catalog;
			// checking if the table to be scanned exists in DuckDB
			if (old_get->GetTable().get() == nullptr) {
				// we are using PostgreSQL (the underlying table does not exist)
				delta_table = "delta_" + dynamic_cast<PostgresBindData *>(old_get->bind_data.get())->table_name;
				delta_table_schema = "public";
				delta_table_catalog = "p"; // todo
			} else {
				// DuckDB (default case)
				delta_table = "delta_" + old_get->GetTable().get()->name;
				delta_table_schema = old_get->GetTable().get()->schema.name;
				delta_table_catalog = old_get->GetTable().get()->catalog.GetName();
			}
			table_catalog_entry =
			    Catalog::GetEntry(context, CatalogType::TABLE_ENTRY, delta_table_catalog, delta_table_schema,
			                      delta_table, OnEntryNotFound::RETURN_NULL, error_context);
			if (table_catalog_entry == nullptr) {
				// if delta base table does not exist, return error
				// this also means there are no deltas to compute
				throw Exception(ExceptionType::BINDER, "Table " + delta_table + " does not exist, no deltas to compute!");
			}

			// we are replacing the GET node with a new GET node that reads the delta table
			// this logic is a bit wonky, the plan should not be executed after these changes
			// however, this is fed to LPTS, which is good enough to generate the query string
			// the previous implementation added a projection on top of the new scan, and scanned all the columns
			// however, I find the code a bit complicated and harder to turn into a string
			// so, now we go with this solution and pray it won't break

			auto &table_entry = table_catalog_entry->Cast<TableCatalogEntry>();
			unique_ptr<FunctionData> bind_data;
			auto scan_function = table_entry.GetScanFunction(context, bind_data);
			vector<LogicalType> return_types = {};
			vector<string> return_names = {};
			vector<column_t> column_ids = {};

			// the delta table has the same columns and column names as the base table, in the same order
			// therefore, we just need to add the columns that we need
			// this is ugly, but needs to stay like this
			// sometimes DuckDB likes to randomly invert columns, so we need to check all of them
			// example: a SELECT * can be translated to 1, 0, 2, 3 rather than 0, 1, 2, 3
			for (auto &id : old_get->GetColumnIds()) {
				column_ids.push_back(id);
				for (auto &col : table_entry.GetColumns().Logical()) {
					if (col.Oid() == id) {
						return_types.push_back(col.Type());
						return_names.push_back(col.Name());
					}
				}
			}

			// we also need to add the multiplicity column
			return_types.push_back(LogicalType::BOOLEAN);
			return_names.push_back("_duckdb_ivm_multiplicity");
			column_ids.push_back(table_entry.GetColumns().GetColumnTypes().size() - 2);

			multiplicity_table_idx = old_get->table_index;
			multiplicity_col_idx = column_ids.size() - 1;

			// we also add the timestamp column
			return_types.push_back(LogicalType::TIMESTAMP);
			return_names.push_back("timestamp");
			column_ids.push_back(table_entry.GetColumns().GetColumnTypes().size() - 1);

			// the new get node that reads the delta table gets a new table index
			auto replacement_get_node = make_uniq<LogicalGet>(old_get->table_index, scan_function, std::move(bind_data),
			                                                  std::move(return_types), std::move(return_names));
			replacement_get_node->SetColumnIds(std::move(column_ids));
			replacement_get_node->table_filters = std::move(old_get->table_filters); // this should be empty

			// we add the filter for the timestamp if there is no filter in the plan
			Connection con(*context.db);
			con.SetAutoCommit(false);
			auto filter_query = "select filter from _duckdb_ivm_views where view_name = '" + view + "'";
			auto r = con.Query(filter_query);
			if (r->HasError()) {
				throw InternalException("Error while querying last_update");
			}

			if (!r->GetValue(0, 0).GetValue<bool>()) {
				// we add a table filter
				auto timestamp_query = "select last_update from _duckdb_ivm_delta_tables where view_name = '" + view + "' and table_name = '" + table_entry.name + "';";
				r = con.Query(timestamp_query);
				if (r->HasError()) {
					throw InternalException("Error while querying last_update");
				}
				auto timestamp_column = make_uniq<BoundColumnRefExpression>(
				    "timestamp", LogicalType::TIMESTAMP,
				    ColumnBinding(multiplicity_table_idx, multiplicity_col_idx + 1));

				auto table_filter = make_uniq<ConstantFilter>(ExpressionType::COMPARE_GREATERTHANOREQUALTO, r->GetValue(0, 0));
				replacement_get_node->table_filters.filters[multiplicity_col_idx + 1] = std::move(table_filter);
			}

			table = table_entry.name; // to use later

			return replacement_get_node;
		}
		case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {

			auto modified_node_logical_agg = dynamic_cast<LogicalAggregate *>(plan.get());
#ifdef DEBUG
			for (size_t i = 0; i < modified_node_logical_agg->GetColumnBindings().size(); i++) {
				printf("aggregate node CB before %zu %s\n", i,
				       modified_node_logical_agg->GetColumnBindings()[i].ToString().c_str());
			}
			printf("Aggregate index: %lu Group index: %lu\n", modified_node_logical_agg->aggregate_index,
			       modified_node_logical_agg->group_index);
#endif

			multiplicity_col_idx = modified_node_logical_agg->groups.size();
			auto mult_group_by =
			    make_uniq<BoundColumnRefExpression>("_duckdb_ivm_multiplicity", LogicalType::BOOLEAN,
			                                        ColumnBinding(multiplicity_table_idx, multiplicity_col_idx));
			modified_node_logical_agg->groups.emplace_back(std::move(mult_group_by));

			auto mult_group_by_stats = make_uniq<BaseStatistics>(BaseStatistics::CreateUnknown(LogicalType::BOOLEAN));
			modified_node_logical_agg->group_stats.emplace_back(std::move(mult_group_by_stats));

			if (modified_node_logical_agg->grouping_sets.empty()) {
				modified_node_logical_agg->grouping_sets = {{0}};
			} else {
				idx_t gr = modified_node_logical_agg->grouping_sets[0].size();
				modified_node_logical_agg->grouping_sets[0].insert(gr);
			}

			multiplicity_table_idx = modified_node_logical_agg->group_index;
#ifdef DEBUG
			for (size_t i = 0; i < modified_node_logical_agg->GetColumnBindings().size(); i++) {
				printf("aggregate node CB after %zu %s\n", i,
				       modified_node_logical_agg->GetColumnBindings()[i].ToString().c_str());
			}
			printf("Modified plan (aggregate/group by):\n%s\nParameters:", plan->ToString().c_str());
			// Output ParameterToString.
			for (const auto& i_param : plan->ParamsToString()) {
				printf("%s", i_param.second.c_str());
			}
			printf("\n---end of modified plan (aggregate/group by)---\n");
#endif
			break;
		}
		case LogicalOperatorType::LOGICAL_PROJECTION: {
			printf("\nIn logical projection case \n Add the multiplicity column to the second node...\n");
			printf("Modified plan (projection, start):\n%s\nParameters:", plan->ToString().c_str());
			for (const auto& i_param : plan->ParamsToString()) {
				printf("%s", i_param.second.c_str());
			}
			printf("\n---end of modified plan (projection)---\n");
			for (size_t i = 0; i < plan->GetColumnBindings().size(); i++) {
				printf("Top node CB before %zu %s\n", i, plan->GetColumnBindings()[i].ToString().c_str());
			}

			auto projection_node = dynamic_cast<LogicalProjection *>(plan.get());
			printf("plan (of projection_node):\n%s\nParameters:", projection_node->ToString().c_str());
			for (const auto& i_param : projection_node->ParamsToString()) {
				printf("%s", i_param.second.c_str());
			}
			printf("\n---end of projection_node plan---\n");

			// the table_idx used to create ColumnBinding will be that of the top node's child
			// the column_idx used to create ColumnBinding for the multiplicity column will be stored using the context from the child
			// node
			auto e = make_uniq<BoundColumnRefExpression>("_duckdb_ivm_multiplicity", LogicalType::BOOLEAN,
			                                             ColumnBinding(multiplicity_table_idx, multiplicity_col_idx));
			printf("Add mult column to exp\n");
			projection_node->expressions.emplace_back(std::move(e));

			printf("Modified plan (of projection_node):\n%s\nParameters:", projection_node->ToString().c_str());
			// Output ParameterToString.
			for (const auto& i_param : projection_node->ParamsToString()) {
				printf("%s", i_param.second.c_str());
			}
			printf("\n---end of modified plan (of projection_node)---\n");
			for (size_t i = 0; i < projection_node->GetColumnBindings().size(); i++) {
				printf("Top node CB %zu %s\n", i, projection_node->GetColumnBindings()[i].ToString().c_str());
			}
			break;
		}
		case LogicalOperatorType::LOGICAL_FILTER: {
			auto filter_node = dynamic_cast<LogicalFilter *>(plan.get());
			filter_node->projection_map.clear();
			// we build another expression for the timestamp
			// the expression is at the topmost level: timestamp >= last_update AND everything else
			// first, we create the timestamp expression
			auto timestamp_query = "select last_update from _duckdb_ivm_delta_tables where view_name = '" + view + "' and table_name = '" + table + "';";
			// converting to timestamp
			Connection con(*context.db);
			con.SetAutoCommit(false);
			auto r = con.Query(timestamp_query);
			if (r->HasError()) {
				throw InternalException("Error while querying last_update");
			}
			auto timestamp_column = make_uniq<BoundColumnRefExpression>("timestamp", LogicalType::TIMESTAMP,
			                                                            ColumnBinding(multiplicity_table_idx, multiplicity_col_idx + 1));
			auto timestamp_expr = make_uniq<BoundComparisonExpression>(
			    ExpressionType::COMPARE_GREATERTHANOREQUALTO,
			    move(timestamp_column),
			    make_uniq<BoundConstantExpression>(r->GetValue(0, 0)));

//			unique_ptr<BoundComparisonExpression> e;

			if (!filter_node->expressions.empty()) {
				auto e = make_uniq<BoundConjunctionExpression>(
				    ExpressionType::CONJUNCTION_AND, std::move(timestamp_expr), std::move(filter_node->expressions[0]));
				filter_node->expressions.clear();
				filter_node->expressions.emplace_back(std::move(e));
			}
			else {
				filter_node->expressions.emplace_back(std::move(timestamp_expr));
			}
			break;
		}
		default:
			throw NotImplementedException("Operator type %s not supported", LogicalOperatorToString(plan->type));
		}
		return std::move(plan);
	}

	static void IVMRewriteRuleFunction(OptimizerExtensionInput &input, duckdb::unique_ptr<LogicalOperator> &plan) {
		// first function call
		// the plan variable contains the plan for "SELECT * FROM DOIVM"
		if (plan->children.empty()) {
			return;
		}

		// check if plan contains table function `DoIVM`
		// The query to trigger IVM will be of the form `CREATE TABLE delta_view_name AS SELECT * from
		// DoIVM('view_name');` The plan's last child should be the DoIVM table function
		auto child = plan.get();
		while (!child->children.empty()) {
			child = child->children[0].get();
		}
		if (child->GetName().substr(0, 5) != "DOIVM") {
			return;
		}

#ifdef DEBUG
		printf("Activating the rewrite rule\n");
#endif

		auto child_get = dynamic_cast<LogicalGet *>(child);
		auto view = child_get->named_parameters["view_name"].ToString();
		auto view_catalog = child_get->named_parameters["view_catalog_name"].ToString();
		auto view_schema = child_get->named_parameters["view_schema_name"].ToString();

		// obtain view definition from catalog
		// generate the optimized logical plan
		Connection con(*input.context.db);

		con.BeginTransaction();
		// todo: maybe we want to disable more optimizers (internal_optimizer_types)
		con.Query("SET disabled_optimizers='compressed_materialization, statistics_propagation, expression_rewriter, filter_pushdown';");
		con.Commit();

		auto v = con.Query("select sql_string from _duckdb_ivm_views where view_name = '" + view + "';");
		if (v->HasError()) {
			throw InternalException("Error while querying view definition");
		}
		string view_query = v->GetValue(0, 0).ToString();

		Parser parser;
		Planner planner(input.context);

		parser.ParseQuery(view_query);
		auto statement = parser.statements[0].get();

		planner.CreatePlan(statement->Copy());

		Optimizer optimizer(*planner.binder, input.context);
		auto optimized_plan = optimizer.Optimize(std::move(planner.plan));
#ifdef DEBUG
		printf("Optimized plan: \n%s\n", optimized_plan->ToString().c_str());
#endif

		// variable to store the column_idx for multiplicity column at each node
		// we do this while creation / modification of the node
		// because this information will not be available while modifying the parent node
		// for ex. parent.children[0] will not contain column names to find the index of the multiplicity column
		idx_t multiplicity_col_idx;
		idx_t multiplicity_table_idx;
		optional_ptr<CatalogEntry> table_catalog_entry = nullptr;

		if (optimized_plan->children.empty()) {
			throw NotImplementedException("Plan contains single node, this is not supported");
		}

		// recursively modify the optimized logical plan

		// IVM does the following steps:
		// 1) Replace the GET (scan) node with a new GET node that reads the delta table
		// 2) Add a filter with the timestamp (only taking the data updated after the last refresh)
		// 3) Add the multiplicity column all other nodes (aggregates etc.)
		// 4) Add the multiplicity column to the top projection node
		// 5) Add the insert node to the plan (to insert the query result in the delta table)

		// if there is no filter, we manually need to add one for the timestamp
		string table;
#ifdef DEBUG
		std::cout << "Running ModifyPlan..." << std::endl;
#endif
		unique_ptr<LogicalOperator> modified_plan = ModifyPlan(
		    input, optimized_plan, multiplicity_col_idx, multiplicity_table_idx, table_catalog_entry, view, table
		);
#ifdef DEBUG
		std::cout << "Running ModifyTopNode..." << std::endl;
#endif
		ModifyTopNode(input.context, modified_plan, multiplicity_col_idx, multiplicity_table_idx);
#ifdef DEBUG
		std::cout << "Running AddInsertNode..." << std::endl;
#endif
		AddInsertNode(input.context, modified_plan, view, view_catalog, view_schema);
#ifdef DEBUG
		std::cout << "\nFINAL PLAN:\n" << modified_plan->ToString() << std::endl;
#endif
		plan = std::move(modified_plan);
		return;
	}
};
}; // namespace duckdb

#endif // DUCKDB_OPENIVM_REWRITE_RULE_HPP
