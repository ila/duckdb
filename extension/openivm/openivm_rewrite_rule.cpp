//
// Created by go on 11/12/24.
//
#include "openivm_rewrite_rule.hpp"

// From DuckDB.
#include "../../compiler/include/logical_plan_to_string.hpp"
#include "../../postgres_scanner/include/postgres_scanner.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb.hpp"

// Std.
#include <iostream>
#include <utility>

namespace duckdb {

void IVMRewriteRule::AddInsertNode(ClientContext &context, unique_ptr<LogicalOperator> &plan,
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

void IVMRewriteRule::ModifyTopNode(
    ClientContext &context,
    unique_ptr<LogicalOperator> &plan,
	ColumnBinding& mul_binding
) {
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
			mul_binding.table_index = dynamic_cast<LogicalAggregate *>(plan->children[0].get())->group_index;
		} else {
		    // TODO: does this break with joins? Or do joins have different logic?
			// this might break with joins
			mul_binding.table_index = dynamic_cast<BoundColumnRefExpression *>(plan->expressions[0].get())->binding.table_index;
		}
		//multiplicity_col_idx = plan->GetColumnBindings().size();
		auto e = make_uniq<BoundColumnRefExpression>("_duckdb_ivm_multiplicity", LogicalType::BOOLEAN, mul_binding);
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

unique_ptr<LogicalOperator> IVMRewriteRule::ModifyPlan(PlanWrapper pw) {
	ClientContext &context = pw.input.context;
	/*
	 * For join support, create a copy of both children for use later.
	 * The reason is that both the delta "state" and the original "state" of each child are needed.
	 * Without a copy, the original (non-delta) state of the children would be lost during recursion.
	 */
	unique_ptr<LogicalOperator> left_child, right_child;
	if (pw.plan.get()->type == LogicalOperatorType::LOGICAL_JOIN ||
	    pw.plan.get()->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		left_child = pw.plan->children[0]->Copy(context);
		right_child = pw.plan->children[1]->Copy(context);
	}
	// Call each child of `plan` recursively (depth-first).
	for (auto &&child : pw.plan->children) {
		auto rec_pw = PlanWrapper(pw.input, child, pw.mul_binding, pw.view, pw.root);
		child = ModifyPlan(rec_pw);
	}
	QueryErrorContext error_context = QueryErrorContext();

	// Rewrite operators depending on their type.
	switch (pw.plan->type) {
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_JOIN: {
		auto join_dl_dr = static_cast<LogicalJoin*>(pw.plan.get());
		printf("Modified plan (join, start, post-cast):\n%s\nParameters:", join_dl_dr->ToString().c_str());
		for (const auto& i_param : join_dl_dr->ParamsToString()) {
			printf("%s", i_param.second.c_str());
		}
#ifdef DEBUG
		printf("join detected. join child count: %zu\n", join_dl_dr->children.size());
		printf("plan left_child child count: %zu\n", left_child->children.size());
		printf("plan right_child child count: %zu\n", right_child->children.size());
#endif
		if (join_dl_dr->join_type != JoinType::INNER) {
			throw Exception(ExceptionType::OPTIMIZER, JoinTypeToString(join_dl_dr->join_type) + " type not yet supported in OpenIVM");
		}
		/* Suppose the query tree (below this join) as an L-side (left child) and an R side (right child).
		 * Before any modification, the query simply joins "current" L with "current" R.
		 * However, now, this query should act as an *update* to the original view.
		 * To this end, a comparison between `current L` and `current R` is not needed.
		 * Rather, this query should yield the union of the following result sets:
		 * 1. `delta R` joined with `current R` -> `join_dl_r`
		 * 2. `current L` joined with `delta R` -> `join_l_dr`
		 * 3. `delta L` joined with `delta R`, iff the multiplicity matches -> `join`
		 *
		 * The code below adapts the tree such that `current L` JOIN `current R` is modified to the 3 joins of interest.
		 */
		// Make a copy of a join between deltas. This copy will eventually become `delta L` with `current R`.
		auto join_dl_r = unique_ptr_cast<LogicalOperator, LogicalJoin>(join_dl_dr->Copy(context));
		// Remove the deltas: join_dl_r temporarily has no children.
		auto left_delta = std::move(join_dl_r->children[0]);
		auto right_delta = std::move(join_dl_r->children[1]);
		// Copy the (now child-less) join, to form the basis for `join_l_dr`.
		auto join_l_dr = unique_ptr_cast<LogicalOperator, LogicalJoin>(join_dl_r->Copy(context));
		// The return type should be the same for each join, so make a copy (to apply below).
		auto types = join_dl_dr->types;

		// Give these two joins their appropriate children.
		join_dl_r->children[0] = std::move(left_delta);
		join_dl_r->children[1] = std::move(right_child);
		join_dl_r->types = types;
		join_l_dr->children[0] = std::move(left_child);
		join_l_dr->children[1] = std::move(right_delta);
		join_l_dr->types = types;
#ifdef DEBUG
		printf("`delta L` JOIN `R` child count: %zu\n", join_dl_r->children.size());
		printf("`L` JOIN `delta R` child count: %zu\n", join_l_dr->children.size());
		printf("`delta L` JOIN `delta R` child count: %zu\n", join_dl_dr->children.size());
#endif


		// dLR -> project dL-mul to end.
		// First, get the table index of whatever is on the left side.
		vector<ColumnBinding> dl_bindings = join_dl_r->children[0]->GetColumnBindings();
		vector<ColumnBinding> r_bindings = join_dl_r->children[1]->GetColumnBindings();
		// Contains the column IDs of within the LEFT side of the projection.
		// From there, we want everything but the last element to stay in order.
		size_t dl_col_count = dl_bindings.size();
		size_t r_col_count = r_bindings.size();
		size_t projection_col_count = dl_col_count + r_col_count;
		auto projection_bindings = vector<ColumnBinding>(projection_col_count); // Note: slots already made.
		// TODO: Do we need to do anything with the `left/right_projection_map`?
		/*
		 * What is done here:
		 * For all except of the last element of L's column bindings,
		 *  the respective ColumnBinding is added to the projection.
		 * The last element of L's bindings (the multiplicity column) is specially kept to insert last.
		 */
		// Mind the `-1`: the last element does not get added yet but only at the very end.
		idx_t last_dl_col_idx = dl_col_count - 1;
		for (idx_t i = 0; i < last_dl_col_idx; ++i ) {
			projection_bindings[i] = dl_bindings[i];
		}
		// Insert the multiplicity column at the end.
		projection_bindings[projection_col_count - 1] = dl_bindings[last_dl_col_idx];
		// Now insert everything of R. Here, everything is inserted, so no special increment cutoff.
		for (idx_t i = 0; i < r_col_count; ++i) {
			// First index here should be where dL left off.
			// Omitting the mul column, that is therefore `last_dl_col_idx` (which is unoccupied).
			// Note that `i = 0`, and thus `last_dl_col_idx + i` = `last_dl_col_idx` (which is intended).
			projection_bindings[last_dl_col_idx + i] = r_bindings[i];
		}
		// Now, the vector with bindings should be complete. Let's put it in a Projection node!
//		auto projection_node = make_uniq<LogicalProjection>(pw.input.optimizer.binder.GenerateTableIndex());

		join_dl_dr->left_projection_map; // Get everything here but the last element.
		// TODO: set the types somehow.



		// LdR -> keep as-is

		// dLdR -> project out dL-mul

		auto copy_union = make_uniq<LogicalSetOperation>(pw.input.optimizer.binder.GenerateTableIndex(), types.size(), std::move(join_dl_r),
														 std::move(join_l_dr), LogicalOperatorType::LOGICAL_UNION, true);
		copy_union->types = types;
		auto upper_u_table_index = pw.input.optimizer.binder.GenerateTableIndex();
		pw.plan = make_uniq<LogicalSetOperation>(upper_u_table_index, types.size(), std::move(copy_union),
														 std::move(pw.plan), LogicalOperatorType::LOGICAL_UNION, true);
		pw.plan->types = types;
		printf("Modified plan (join, end):\n%s\nParameters:", pw.plan->ToString().c_str());
		for (const auto& i_param : pw.plan->ParamsToString()) {
			printf("%s", i_param.second.c_str());
		}
		// TODO: Rebind everything, because new joins have been implemented.
		/*
		ColumnBindingReplacer replacer;
		auto &replacement_bindings = replacer.replacement_bindings;
		const auto bindings = plan->GetColumnBindings();
		for (idx_t col_idx = 0; col_idx < bindings.size(); col_idx++) {
			const auto &old_binding = bindings[col_idx];
			const auto &new_binding = ColumnBinding(upper_u_table_index, old_binding.column_index);
			replacement_bindings.emplace_back(old_binding, new_binding);
		}
		replacer.stop_operator = plan;
		replacer.VisitOperator(*root);
		*/
		break;
	}
	case LogicalOperatorType::LOGICAL_GET: {
		// we are at the bottom of the tree
		auto old_get = dynamic_cast<LogicalGet *>(pw.plan.get());

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
		auto table_catalog_entry =
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

		pw.mul_binding.table_index = old_get->table_index;
		pw.mul_binding.column_index = column_ids.size() - 1;
		// Instead of manually modifying table and column, could also just create a new ColumnBinding:
		//pw.mul_binding = ColumnBinding(old_get->table_index, column_ids.size() - 1);

		// we also add the timestamp column
		return_types.push_back(LogicalType::TIMESTAMP);
		return_names.push_back("timestamp");
		column_ids.push_back(table_entry.GetColumns().GetColumnTypes().size() - 1);

		// the new get node that reads the delta table gets a new table index
		auto replacement_get_node = make_uniq<LogicalGet>(old_get->table_index, scan_function, std::move(bind_data),
														  std::move(return_types), std::move(return_names));
		replacement_get_node->SetColumnIds(std::move(column_ids));
		replacement_get_node->table_filters = std::move(old_get->table_filters); // this should be empty

		// FIXME: Why add the filter if there is no filter in the plan???
		// we add the filter for the timestamp if there is no filter in the plan
		Connection con(*context.db);
		con.SetAutoCommit(false);
		// we add a table filter
		auto timestamp_query = "select last_update from _duckdb_ivm_delta_tables where view_name = '" + pw.view + "' and table_name = '" + table_entry.name + "';";
		auto r = con.Query(timestamp_query);
		if (r->HasError()) {
			throw InternalException("Error while querying last_update");
		}
		auto timestamp_column = make_uniq<BoundColumnRefExpression>(
			"timestamp", LogicalType::TIMESTAMP,
			ColumnBinding(pw.mul_binding.table_index, pw.mul_binding.column_index + 1));

		auto table_filter = make_uniq<ConstantFilter>(ExpressionType::COMPARE_GREATERTHANOREQUALTO, r->GetValue(0, 0));
		replacement_get_node->table_filters.filters[pw.mul_binding.column_index + 1] = std::move(table_filter);

		return replacement_get_node;
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {

		auto modified_node_logical_agg = dynamic_cast<LogicalAggregate *>(pw.plan.get());
#ifdef DEBUG
		for (size_t i = 0; i < modified_node_logical_agg->GetColumnBindings().size(); i++) {
			printf("aggregate node CB before %zu %s\n", i,
				   modified_node_logical_agg->GetColumnBindings()[i].ToString().c_str());
		}
		printf("Aggregate index: %lu Group index: %lu\n", modified_node_logical_agg->aggregate_index,
			   modified_node_logical_agg->group_index);
#endif

		pw.mul_binding.column_index = modified_node_logical_agg->groups.size();
		auto mult_group_by =
			make_uniq<BoundColumnRefExpression>("_duckdb_ivm_multiplicity", LogicalType::BOOLEAN, pw.mul_binding);
		modified_node_logical_agg->groups.emplace_back(std::move(mult_group_by));

		auto mult_group_by_stats = make_uniq<BaseStatistics>(BaseStatistics::CreateUnknown(LogicalType::BOOLEAN));
		modified_node_logical_agg->group_stats.emplace_back(std::move(mult_group_by_stats));

		if (modified_node_logical_agg->grouping_sets.empty()) {
			modified_node_logical_agg->grouping_sets = {{0}};
		} else {
			idx_t gr = modified_node_logical_agg->grouping_sets[0].size();
			modified_node_logical_agg->grouping_sets[0].insert(gr);
		}

		pw.mul_binding.table_index = modified_node_logical_agg->group_index;
#ifdef DEBUG
		for (size_t i = 0; i < modified_node_logical_agg->GetColumnBindings().size(); i++) {
			printf("aggregate node CB after %zu %s\n", i,
				   modified_node_logical_agg->GetColumnBindings()[i].ToString().c_str());
		}
		printf("Modified plan (aggregate/group by):\n%s\nParameters:", pw.plan->ToString().c_str());
		// Output ParameterToString.
		for (const auto& i_param : pw.plan->ParamsToString()) {
			printf("%s", i_param.second.c_str());
		}
		printf("\n---end of modified plan (aggregate/group by)---\n");
#endif
		break;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		printf("\nIn logical projection case \n Add the multiplicity column to the second node...\n");
		printf("Modified plan (projection, start):\n%s\nParameters:", pw.plan->ToString().c_str());
		for (const auto& i_param : pw.plan->ParamsToString()) {
			printf("%s", i_param.second.c_str());
		}
		printf("\n---end of modified plan (projection)---\n");
		for (size_t i = 0; i < pw.plan->GetColumnBindings().size(); i++) {
			printf("Top node CB before %zu %s\n", i, pw.plan->GetColumnBindings()[i].ToString().c_str());
		}

		auto projection_node = dynamic_cast<LogicalProjection *>(pw.plan.get());
		printf("plan (of projection_node):\n%s\nParameters:", projection_node->ToString().c_str());
		for (const auto& i_param : projection_node->ParamsToString()) {
			printf("%s", i_param.second.c_str());
		}
		printf("\n---end of projection_node plan---\n");

		// the table_idx used to create ColumnBinding will be that of the top node's child
		// the column_idx used to create ColumnBinding for the multiplicity column will be stored using the context from the child
		// node
		auto e = make_uniq<BoundColumnRefExpression>("_duckdb_ivm_multiplicity", LogicalType::BOOLEAN, pw.mul_binding);
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
		break;
	}
	default:
		throw NotImplementedException("Operator type %s not supported", LogicalOperatorToString(pw.plan->type));
	}
	return std::move(pw.plan);
}
void IVMRewriteRule::IVMRewriteRuleFunction(OptimizerExtensionInput &input, duckdb::unique_ptr<LogicalOperator> &plan) {
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
	ColumnBinding mul_binding;
	optional_ptr<CatalogEntry> table_catalog_entry = nullptr; // TODO: 2024-12-13 set but not used

	if (optimized_plan->children.empty()) {
		throw NotImplementedException("Plan contains single node, this is not supported");
	}

	/* +++ Old logic. (outdated as of December 2024) +++
	// recursively modify the optimized logical plan
	// IVM does the following steps:
	// 1) Replace the GET (scan) node with a new GET node that reads the delta table
	// 2) Add a filter with the timestamp (only taking the data updated after the last refresh)
	// 3) Add the multiplicity column all other nodes (aggregates etc.)
	// 4) Add the multiplicity column to the top projection node
	// 5) Add the insert node to the plan (to insert the query result in the delta table)
	// if there is no filter, we manually need to add one for the timestamp
    */

	/* The IVM logic takes the following steps:
	 * 1. Replace the GET (scan) node with a new GET note, reading the delta table.
	 * 2. Add a timestamp filter expression to this new GET note
	 *     (i.e. only consider records updated after the last refresh)
	 * 3. Recursively add the multiplicity column upwards into the tree.
	 * 4. Replace any occurrences of joins with 3 joins (permutations with a delta on either/both sides)
	 * 5. Add an insert node to the top of the plan, such that the result can be inserted into the delta table.
	 */

#ifdef DEBUG
	std::cout << "Running ModifyPlan..." << '\n';
#endif
	auto root = optimized_plan.get();
	auto start_pw = PlanWrapper(input, optimized_plan, mul_binding, view, root);
	unique_ptr<LogicalOperator> modified_plan = ModifyPlan(start_pw);
#ifdef DEBUG
	std::cout << "Running ModifyTopNode..." << '\n';
#endif
	ModifyTopNode(input.context, modified_plan, mul_binding);
#ifdef DEBUG
	std::cout << "Running AddInsertNode..." << '\n';
#endif
	AddInsertNode(input.context, modified_plan, view, view_catalog, view_schema);
#ifdef DEBUG
	std::cout << "\nFINAL PLAN:\n" << modified_plan->ToString() << '\n';
#endif
	plan = std::move(modified_plan);
	return;
}
} // namespace duckdb.