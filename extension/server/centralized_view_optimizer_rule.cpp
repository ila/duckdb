#include "include/centralized_view_optimizer_rule.hpp"

namespace duckdb {

void CVRewriteRule::CVRewriteRuleFunction(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	// first function call
	// the plan variable contains the plan for "SELECT * FROM DOIVM"
	if (plan->children.empty()) {
		return;
	}

	auto child = plan.get();
	while (!child->children.empty()) {
		child = child->children[0].get();
	}
	if (child->GetName().substr(0, 5) != "?????") { // todo
		return;
	}
}
}
