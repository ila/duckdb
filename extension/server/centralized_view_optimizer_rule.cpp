#include "include/centralized_view_optimizer_rule.hpp"
#include <duckdb/common/printer.hpp>

namespace duckdb {

void CVRewriteRule::CVRewriteRuleFunction(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	// first function call
	// the plan variable contains the plan for "SELECT * FROM Flush('view_name');"
	if (plan->children.empty()) {
		return;
	}
	if (plan->GetName().substr(0, 5) == "flush") {
		Printer::Print("Here!");
		auto child = plan.get();
		while (!child->children.empty()) {
			child = child->children[0].get();
		}
	} else {
		return;
	}
}
}
