#ifndef DUCKDB_OPENIVM_REWRITE_RULE_HPP
#define DUCKDB_OPENIVM_REWRITE_RULE_HPP

#include "duckdb.hpp"

namespace duckdb {

/// The input parameter for IVMRewriteRule::ModifyPlan. Used to simplify the recursive calls.
struct PlanWrapper {
	PlanWrapper(OptimizerExtensionInput &input_, unique_ptr<LogicalOperator> &plan_, string &view_,
	            LogicalOperator *&root_)
	    : input(input_), plan(plan_), view(view_), root(root_) {
	}
	// Everything should be a reference, as it is used later.
	/// Contains the query context, and general optimizer information.
	OptimizerExtensionInput &input;
	/// The (subtree) that should be worked on.
	unique_ptr<LogicalOperator> &plan;
	/// TODO: Give this a description.
	string &view;
	/// The root of the query tree. Needed for a ColumnBinding replacer.
	LogicalOperator *&root;
	/// The logical type of the multiplicity column. Should always be a boolean.
	const LogicalType mul_type = LogicalType::BOOLEAN;
};

/// The output parameter for IVMRewriteRule::ModifyPlan.
/// Used to combine a LogicalOperator with the ColumnBinding of the multiplicity column.
struct ModifiedPlan {
	ModifiedPlan(unique_ptr<LogicalOperator> op_, ColumnBinding mul_binding_)
	    : op(std::move(op_)), mul_binding(mul_binding_) {
	}

	/// The operator of the modified plan.
	unique_ptr<LogicalOperator> op;
	/// The multiplicity binding of the modified plan (should always be defined).
	ColumnBinding mul_binding;
};

class IVMRewriteRule : public OptimizerExtension {
public:
	IVMRewriteRule() {
		optimize_function = IVMRewriteRuleFunction;
	}

	/// Add an Insert node to the top of the plan, such that an update on the view can be performed.
	static void AddInsertNode(ClientContext &context, unique_ptr<LogicalOperator> &plan, string &view_name,
	                          string &view_catalog_name, string &view_schema_name);

	static ModifiedPlan ModifyPlan(PlanWrapper pw);

	static void IVMRewriteRuleFunction(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);
};

} // namespace duckdb

#endif // DUCKDB_OPENIVM_REWRITE_RULE_HPP
