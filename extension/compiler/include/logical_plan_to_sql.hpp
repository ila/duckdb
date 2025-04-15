//
// Created by sppub on 09/04/2025.
//

#ifndef LOGICAL_PLAN_TO_SQL_HPP
#define LOGICAL_PLAN_TO_SQL_HPP

#include "duckdb.hpp"

namespace duckdb {

/// Base node for the intermediate representation (IR) of a SQL query. Virtual class.
class IRNode {
public:
    virtual ~IRNode() = default;
	virtual std::string ToQuery() = 0;
	// Constructor.
	explicit IRNode(const size_t index ) : idx(index) {}
	// virtual void ToAst() = 0;  // Will be needed for ASTs.
	const size_t idx;  // Number of the node used for giving it a name.
};

/// Node for insertion queries. Cannot be a CTE.
// TODO: Create constructor, and implement support in LPTsql.
class InsertNode: public IRNode {
	// Attributes.
	std::string target_table;
	std::string child_cte_name;  // If "insert into t_name values (...)", not defined.
	OnConflictAction action_type;
public:
	~InsertNode() override = default;
	// Constructor.
	InsertNode(
		const size_t index, std::string _target_table, std::string _child_cte_name, const OnConflictAction conflict_action_type
	) :
		IRNode(index),
		target_table(std::move(_target_table)),
		child_cte_name(std::move(_child_cte_name)),
		action_type(conflict_action_type) {}
	// Functions.
	std::string ToQuery() override;
};

/// Node for update queries. Cannot be a CTE.
// Not (yet) needed for IVM.
class UpdateNode: public IRNode {
public:
	~UpdateNode() override = default;
};

/// Node for deletion queries. Cannot be a CTE.
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
	explicit CteNode(const size_t index, std::string name, vector<std::string> col_list)
	: IRNode(index), cte_name(std::move(name)), cte_column_list(std::move(col_list)) {}
	// Requires ToQuery() to be implemented by derived classes.
	/// Create a CTE-like string for the Node (excluding the WITH keyword).
	std::string ToCteQuery();
	// Attributes.
	/// The name of the CTE (so what comes after WITH).
	std::string cte_name;
	/// The "external" names of the CTE columns (the name ancestors need to access columns).
	vector<std::string> cte_column_list;
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
		vector<std::string> cte_column_names,
		std::string _catalog,
		std::string _schema,
		std::string _table_name,
		const size_t _table_index,
		vector<string> _table_filters,
		vector<string> _column_names
    ) :
		CteNode(index, "scan_" + std::to_string(index), std::move(cte_column_names)),
		catalog(std::move(_catalog)),
		schema(std::move(_schema)),
		table_name(std::move(_table_name)),
		table_index(_table_index),
		table_filters(std::move(_table_filters)),
		column_names(std::move(_column_names)) {}
	// Functions.
	std::string ToQuery() override;
};

class FilterNode: public CteNode {
	// Attributes.
	std::string child_cte_name;
	vector<std::string> conditions;
public:
	~FilterNode() override = default;
	// Constructor.
	FilterNode(
		const size_t index,
		vector<std::string> cte_column_names,
		std::string _child_cte_name,
		vector<std::string> _conditions
	) :
		CteNode(index, "filter_" + std::to_string(index), std::move(cte_column_names)),
		child_cte_name(std::move(_child_cte_name)),
		conditions(std::move(_conditions)) {}
	// Functions.
	std::string ToQuery() override;
};

class ProjectNode: public CteNode {
	// Attributes.
	std::string child_cte_name;
	vector<std::string> column_names;
	size_t table_index;
public:
	~ProjectNode() override = default;
	// Constructor.
	ProjectNode(
		const size_t index,
		vector<std::string> cte_column_names,
		std::string _child_cte_name,
		vector<std::string> _column_names,
		const size_t _table_index
	) :
		CteNode(index, "projection_" + std::to_string(index), std::move(cte_column_names)),
		child_cte_name(std::move(_child_cte_name)),
		column_names(std::move(_column_names)),
		table_index(_table_index) {}
	// Functions.
	std::string ToQuery() override;
};

class AggregateNode: public CteNode {
	// Attributes.
	std::string child_cte_name;
	vector<std::string> group_names; // If empty, is scalar aggregate.
	vector<std::string> aggregate_names;
public:
	~AggregateNode() override = default;
	// Constructor.
	AggregateNode(
		const size_t index,
		vector<std::string> cte_column_names,
		std::string _child_cte_name,
		vector<std::string> _group_names,
		vector<std::string> _aggregate_names
	) :
		CteNode(index, "aggregate_" + std::to_string(index), std::move(cte_column_names)),
		child_cte_name(std::move(_child_cte_name)),
		group_names(std::move(_group_names)),
		aggregate_names(std::move(_aggregate_names)) {}
	// Functions.
	std::string ToQuery() override;
};

class JoinNode: public CteNode {
	// Attributes.
	std::string left_cte_name, right_cte_name;
	JoinType join_type;
	vector<std::string> join_conditions;
public:
	~JoinNode() override = default;
	// Constructor.
	JoinNode(
		const size_t index,
		vector<std::string> cte_column_names,
		std::string _left_cte_name,
		std::string _right_cte_name,
		JoinType _join_type,
		vector<std::string> _join_conditions) :
		CteNode(index, "join_" + std::to_string(index), std::move(cte_column_names)),
		left_cte_name(std::move(_left_cte_name)),
		right_cte_name(std::move(_right_cte_name)),
		join_type(_join_type),
		join_conditions(std::move(_join_conditions)) {}
	// Functions.
	std::string ToQuery() override;
};

class UnionNode: public CteNode {
	// Attributes.
	std::string left_cte_name;
	std::string right_cte_name;
	const bool is_union_all; // Whether to use "UNION ALL" or just "UNION".
public:
	~UnionNode() override = default;
	// Constructor.
	UnionNode(
		const size_t index,
		vector<std::string> cte_column_names,
		std::string _left_cte_name,
		std::string _right_cte_name,
		const bool union_all
	) :
		CteNode(index, "union_" + std::to_string(index), std::move(cte_column_names)),
		left_cte_name(std::move(_left_cte_name)),
		right_cte_name(std::move(_right_cte_name)),
		is_union_all(union_all) {}
	// Functions.
	std::string ToQuery() override;
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
	/// Create a DuckDB SQL query directly from the immediate representation.
	/// If `use_newlines` is true, the string uses newlines between CTEs for readability.
	std::string ToQuery(bool use_newlines);
};

class LogicalPlanToSql {
private:
	/// Struct with a ColumnBinding that implements `<`, such that using it in a map becomes possible.
	/// Needed for `column_map`.
	struct MappableColumnBinding {
		ColumnBinding cb;
		MappableColumnBinding(const ColumnBinding _column_binding) : cb(std::move(_column_binding)) {}

		bool operator<(const MappableColumnBinding& other) const {
			return std::tie(cb.table_index, cb.column_index) < std::tie(other.cb.table_index, other.cb.column_index);
		}
	};
	struct ColStruct {
		const idx_t table_index;
		std::string column_name;
		std::string alias; // Optional. Empty string if not defined.
		// Constructor.
		ColStruct(const idx_t _table_index, std::string _column_name, std::string _alias) :
			table_index(_table_index), column_name(std::move(_column_name)), alias(std::move(_alias)) {}
		/// Generate a column name for this ColStruct. Uses `alias` if defined; `column_name` otherwise.
		/// Example format: t7_amount
		std::string ToUniqueColumnName() const;
	};

	// Input to the class, used to traverse the query plan.
	ClientContext &context;
	/// The tree that should be converted to an AST.
	unique_ptr<LogicalOperator> &plan;

	/// Used to enumerate the CTEs.
	size_t node_count = 0;
	/// Used to eventually create the IRStruct object needed for the IR.
	/// Becomes a nullptr once LogicalPlanToIR has finished.
	vector<unique_ptr<CteNode>> cte_nodes;
	/// Used for consistent column naming across all nodes.
	std::map<MappableColumnBinding, unique_ptr<ColStruct>> column_map;

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
	static std::string IRToSql(unique_ptr<IRStruct> &ir_struct);
};

} // namespace duckdb

#endif //LOGICAL_PLAN_TO_SQL_HPP
