#ifndef DUCKDB_OPENIVM_REWRITE_RULE_HPP
#define DUCKDB_OPENIVM_REWRITE_RULE_HPP

#include "duckdb.hpp"

namespace duckdb {

/// The input parameter for IVMRewriteRule::ModifyPlan. Used to simplify the recursive calls.
struct PlanWrapper {
	PlanWrapper(
	    OptimizerExtensionInput& input_,
		unique_ptr<LogicalOperator>& plan_,
		ColumnBinding& multiplicity_binding_,
		string& view_,
		LogicalOperator*& root_
	) : input(input_), plan(plan_), mul_binding(multiplicity_binding_), view(view_), root(root_)
	{}
	// Everything should be a reference, as it is used later.
	/// Contains the query context, and general optimizer information.
	OptimizerExtensionInput& input;
	/// The (subtree) that should be worked on.
	unique_ptr<LogicalOperator>& plan;
	/// The binding corresponding to the multiplicity column.
	ColumnBinding& mul_binding; // Previously, &multiplicity_col_idx and &multiplicity_table_idx were separate.
	string& view;
	/// The root of the query tree. Needed for a ColumnBinding replacer.
	LogicalOperator*& root;
};

class IVMRewriteRule : public OptimizerExtension {
public:
	IVMRewriteRule() {
		optimize_function = IVMRewriteRuleFunction;
	}

	static void AddInsertNode(
	    ClientContext &context,
	    unique_ptr<LogicalOperator> &plan,
	    string &view_name,
	    string &view_catalog_name,
	    string &view_schema_name
	);

	static void ModifyTopNode(ClientContext &context, unique_ptr<LogicalOperator> &plan, ColumnBinding& mul_binding);

	static unique_ptr<LogicalOperator> ModifyPlan(PlanWrapper pw);

	static void IVMRewriteRuleFunction(OptimizerExtensionInput &input, duckdb::unique_ptr<LogicalOperator> &plan);
};

} // namespace duckdb

#endif // DUCKDB_OPENIVM_REWRITE_RULE_HPP
