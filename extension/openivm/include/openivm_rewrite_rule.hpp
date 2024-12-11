#ifndef DUCKDB_OPENIVM_REWRITE_RULE_HPP
#define DUCKDB_OPENIVM_REWRITE_RULE_HPP

#include "duckdb.hpp"

namespace duckdb {

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

	static void ModifyTopNode(
	    ClientContext &context,
	    unique_ptr<LogicalOperator> &plan,
	    idx_t &multiplicity_col_idx,
	    idx_t &multiplicity_table_idx
	);

	static unique_ptr<LogicalOperator> ModifyPlan(
	    OptimizerExtensionInput &input,
	    unique_ptr<LogicalOperator> &plan,
		idx_t &multiplicity_col_idx, // create a struct along with table index ("ColumnBinding").
	    idx_t &multiplicity_table_idx,
	    string &view,
	    LogicalOperator* &root
	);

	static void IVMRewriteRuleFunction(OptimizerExtensionInput &input, duckdb::unique_ptr<LogicalOperator> &plan);
};
} // namespace duckdb

#endif // DUCKDB_OPENIVM_REWRITE_RULE_HPP
