//
// Created by sppub on 09/04/2025.
//

#ifndef LOGICAL_PLAN_TO_SQL_HPP
#define LOGICAL_PLAN_TO_SQL_HPP

#include "duckdb.hpp"

namespace duckdb {

struct IRNode {
    virtual ~IRNode() = default;
	size_t idx;  // Number of the node used for giving it a name.
};

struct InsertNode: public IRNode {
	std::string query;
};

// Not (yet) needed for IVM.
struct UpdateNode: public IRNode {
};

// Not (yet) needed for IVM.
struct DeleteNode: public IRNode {
};

struct CteNode: public IRNode {
	std::string cte_name;
};

struct GetNode: public CteNode {
	std::string catalog;
	std::string schema;
	std::string table_name;
	size_t table_index;
	vector<string> table_filters;
	vector<string> column_names;
};

struct FilterNode: public CteNode {
	vector<string> conditions;
};

struct ProjectNode: public CteNode {
	vector<string> column_names;
	size_t table_index;
};

struct AggregateNode: public CteNode {
	vector<string> group_names; // If empty, is scalar aggregate.
	vector<string> aggregate_names;
};

struct JoinNode: public CteNode {
	std::string left_cte_name, right_cte_name;
	std::string join_type; // TODO: convert into Enum/whatever DuckDB uses.
	vector<std::string> join_conditions;
};

struct UnionNode: public CteNode {
	std::string left_cte_name;
	std::string right_cte_name;
	bool is_union_all; // Whether to use "UNION ALL" or just "UNION".
};

struct CteVec {
	vector<CteNode> nodes;
	IRNode finial_node;
};

class LogicalPlanToSql {
private:
	// Input to the class, used to traverse the query plan.
	ClientContext &context;
	/// The tree that should be converted to an AST.
	unique_ptr<LogicalOperator> &plan;

	/// Used to enumerate the CTEs.
	size_t node_count = 0;
	// Used to eventually create the CteVec object needed for the IR.
	vector<CteNode> cte_nodes;
	CteVec cte_vec;

	/// Create a CTE from a LogicalOperator.
	CteNode CreateCteNode(unique_ptr<LogicalOperator> &subplan, const vector<size_t>& children_indices);
	/// Traverse the logical plan recursively
	CteNode RecursiveTraversal(unique_ptr<LogicalOperator> &subplan, const bool is_root);

public:
	LogicalPlanToSql(ClientContext &_context, unique_ptr<LogicalOperator> &_plan)
	  : context(_context), plan(_plan) {}

	void LogicalPlanToIR();
	void IRToAst();
	void AstToSql();

};




} // namespace duckdb

#endif //LOGICAL_PLAN_TO_SQL_HPP
