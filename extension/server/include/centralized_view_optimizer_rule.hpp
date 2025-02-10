//
// Created by ila on 2/7/25.
//

#ifndef CENTRALIZED_VIEW_OPTIMIZER_RULE_HPP
#define CENTRALIZED_VIEW_OPTIMIZER_RULE_HPP
#include <duckdb/optimizer/optimizer_extension.hpp>

namespace duckdb {
class CVRewriteRule : public OptimizerExtension {
public:
	CVRewriteRule() {
		optimize_function = CVRewriteRuleFunction;
	}

	static void CVRewriteRuleFunction(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);
};
}

#endif //CENTRALIZED_VIEW_OPTIMIZER_RULE_HPP
