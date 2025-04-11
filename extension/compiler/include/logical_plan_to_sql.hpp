//
// Created by sppub on 09/04/2025.
//

#ifndef LOGICAL_PLAN_TO_SQL_HPP
#define LOGICAL_PLAN_TO_SQL_HPP

#include "duckdb.hpp"

namespace duckdb {

class IRNode {
public:
    virtual ~IRNode() = default;
	virtual std::string ToQuery() = 0;
	// Constructor.
	explicit IRNode(const size_t index ) : idx(index) {}
	// virtual void ToAst() = 0;  // Will be needed for ASTs.
	const size_t idx;  // Number of the node used for giving it a name.
};

class InsertNode: public IRNode {
public:
	~InsertNode() override = default;
	std::string query;
};

// Not (yet) needed for IVM.
class UpdateNode: public IRNode {
public:
	~UpdateNode() override = default;
};

// Not (yet) needed for IVM.
class DeleteNode: public IRNode {
public:
	~DeleteNode() override = default;
};

class CteNode: public IRNode {
public:
	~CteNode() override = default;
	// Explicitly delete copy constructor to avoid issues.
	CteNode(const CteNode &) = delete;
	CteNode& operator=(const CteNode &) = delete;
	// Constructor.
	explicit CteNode(const size_t index, std::string name) : IRNode(index), cte_name(std::move(name)) {}
	// To be implemented by derived classes.
	virtual std::string ToCteQuery() = 0;
	// Attributes.
	std::string cte_name;
};

class GetNode: public CteNode {
	// Attributes.
	std::string catalog;
	std::string schema;
	std::string table_name;
	size_t table_index;
	vector<std::string> table_filters;
	vector<std::string> column_names;
public:
	~GetNode() override = default;
	// Constructor. TODO: Should be explicit or not?
	GetNode(
		const size_t index,
		std::string _catalog,
		std::string _schema,
		std::string _table_name,
		const size_t _table_index,
		vector<string> _table_filters,
		vector<string> _column_names
    ) : CteNode(index, "scan_" + std::to_string(index)),
		catalog(std::move(_catalog)),
		schema(std::move(_schema)),
		table_name(std::move(_table_name)),
		table_index(_table_index),
		table_filters(std::move(_table_filters)),
		column_names(std::move(_column_names)) {}
	// Functions.
	std::string ToQuery() override {return "todo: implement";};
	std::string ToCteQuery() override {return "todo: implement";};
};

class FilterNode: public CteNode {
	// Attributes.
	vector<std::string> conditions;
public:
	~FilterNode() override = default;
	// Constructor.
	FilterNode(const size_t index, vector<std::string> _conditions) :
		CteNode(index, "filter_" + std::to_string(index)), conditions(std::move(_conditions)) {}
	// Functions.
	std::string ToQuery() override {return "todo: implement";};
	std::string ToCteQuery() override {return "todo: implement";};

};

class ProjectNode: public CteNode {
	// Attributes.
	vector<std::string> column_names;
	size_t table_index;
public:
	~ProjectNode() override = default;
	// Constructor.
	ProjectNode(const size_t index, vector<std::string> _column_names, const size_t _table_index) :
		CteNode(index, "projection_" + std::to_string(index)),
		column_names(std::move(_column_names)),
		table_index(_table_index) {}
	// Functions.
	std::string ToQuery() override {return "todo: implement";};
	std::string ToCteQuery() override {return "todo: implement";};


};

class AggregateNode: public CteNode {
	// Attributes.
	vector<std::string> group_names; // If empty, is scalar aggregate.
	vector<std::string> aggregate_names;
public:
	~AggregateNode() override = default;
	// Constructor.
	AggregateNode(const size_t index, vector<std::string> _group_names, vector<std::string> _aggregate_names) :
		CteNode(index, "aggregate_" + std::to_string(index)),
		group_names(std::move(_group_names)),
		aggregate_names(std::move(_aggregate_names)) {}
	// Functions.
	std::string ToQuery() override {return "todo: implement";};
	std::string ToCteQuery() override {return "todo: implement";};

};

class JoinNode: public CteNode {
	// Attributes.
	std::string left_cte_name, right_cte_name;
	std::string join_type; // TODO: convert into Enum/whatever DuckDB uses.
	vector<std::string> join_conditions;
public:
	~JoinNode() override = default;
	// Constructor.
	JoinNode(
		const size_t index,
		std::string _left_cte_name,
		std::string _right_cte_name,
		std::string _join_type,
		vector<std::string> _join_conditions) :
		CteNode(index, "join_" + std::to_string(index)),
		left_cte_name(std::move(_left_cte_name)),
		right_cte_name(std::move(_right_cte_name)),
		join_type(std::move(_join_type)),
		join_conditions(std::move(_join_conditions)) {}
	// Functions.
	std::string ToQuery() override {return "todo: implement";};
	std::string ToCteQuery() override {return "todo: implement";};

};

class UnionNode: public CteNode {
	// Attributes.
	std::string left_cte_name;
	std::string right_cte_name;
	const bool is_union_all; // Whether to use "UNION ALL" or just "UNION".
public:
	~UnionNode() override = default;
	// Constructor.
	UnionNode(const size_t index, std::string _left_cte_name, std::string _right_cte_name, const bool union_all) :
		CteNode(index, "union_" + std::to_string(index)),
		left_cte_name(std::move(_left_cte_name)),
		right_cte_name(std::move(_right_cte_name)),
		is_union_all(union_all) {}
	// Functions.
	std::string ToQuery() override {return "todo: implement";};
	std::string ToCteQuery() override {return "todo: implement";};

};

/// Intermediate representation (as a vector of CTEs along with a final node).
class IRStruct {
	// Attributes.
	vector<unique_ptr<CteNode>> nodes;
	unique_ptr<IRNode> final_node;
public:
	// Constructor.
	IRStruct(vector<unique_ptr<CteNode>> _nodes, unique_ptr<IRNode> _final_node) :
		nodes(std::move(_nodes)), final_node(std::move(_final_node)) {}
	// Function.
	std::string ToQuery();
};

class LogicalPlanToSql {
private:
	// Input to the class, used to traverse the query plan.
	ClientContext &context;
	/// The tree that should be converted to an AST.
	unique_ptr<LogicalOperator> &plan;

	/// Used to enumerate the CTEs.
	size_t node_count = 0;
	// Used to eventually create the IRStruct object needed for the IR.
	// Becomes a nullptr once LogicalPlanToIR has finished.
	vector<unique_ptr<CteNode>> cte_nodes;

	/// Create a CTE from a LogicalOperator.
	unique_ptr<CteNode> CreateCteNode(unique_ptr<LogicalOperator> &subplan, const vector<size_t>& children_indices);
	/// Traverse the logical plan recursively, except for the root.
	unique_ptr<CteNode> RecursiveTraversal(unique_ptr<LogicalOperator> &sub_plan);

public:
	LogicalPlanToSql(ClientContext &_context, unique_ptr<LogicalOperator> &_plan)
	  : context(_context), plan(_plan) {}

	/// Convert the logical plan to an immediate representation (IRStruct).
	unique_ptr<IRStruct> LogicalPlanToIR();
	/// Convert the IR into an AST (portable to other SQL dialects).
	void IRToAst(); // Not implemented.
	/// Convert an AST into a DuckDB SQL query.
	void AstToSql(); // Not implemented.
	/// Create a DuckDB SQL query directly from the immediate representation (IRStruct).
	static std::string IRToSql(unique_ptr<IRStruct> &ir_struct);

};




} // namespace duckdb

#endif //LOGICAL_PLAN_TO_SQL_HPP
