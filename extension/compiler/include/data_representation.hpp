//
// Created by destrex271(Akshat) 4th Feb, 2024
//
// #pragma once

#ifndef DUCKDB_DATA_REPRESENTATION_HPP
#define DUCKDB_DATA_REPRESENTATION_HPP

#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"

// Naming the representation: DuckAST

namespace duckdb {

// DuckASTBaseOperator Types
enum DuckASTOperatorType { NONE, PROJECTION, GET, FILTER, AGGREGATE, ORDER_BY, INSERT, CROSS_JOIN };

// Node Type Classes
class DuckASTBaseOperator {
	// Rename: Operator
public:
	string name;
	DuckASTBaseOperator();
	DuckASTBaseOperator(const string &name);
	virtual ~DuckASTBaseOperator();
};

class DuckASTProjection : public DuckASTBaseOperator {
public:
	std::map<string, string> column_aliases;
	DuckASTProjection();
	DuckASTProjection(string &name);
	~DuckASTProjection() override;
	void AddColumn(const string &table_index, const string &column_index, const string &alias);
};

class DuckASTFilter : public DuckASTBaseOperator {
public:
	std::string filter_condition;
	DuckASTFilter();
	DuckASTFilter(const string &filter_condition);
	void SetFilterCondition(string &filter_condition);
};

/*
 * DuckASTAggregate is a 1:1 relationship with the LogicalAggregate
 * in src/include/duckdb/planner/logical_aggregate.hpp
 */
class DuckASTAggregate : public DuckASTBaseOperator {
public:
	vector<string> group_column;
	vector<string> aggregate_function;
	bool is_group_by;
	DuckASTAggregate(vector<string> &aggregate_function, vector<string> &group_column);
};

/*
 * DuckASTOrderBy is a 1:1 relationship with the LogicalOrder
 * in src/include/duckdb/planner/logical_order.hpp
 */
class DuckASTOrderBy : public DuckASTBaseOperator {
public:
	// maps column to arrange and the order in which to do it(ascending, descending)
	unordered_map<string, string> order;
	DuckASTOrderBy();
	// appends to the order column
	void AddOrderColumn(const string &col, const string &ord);
};

/*
 * Will support all the joins like Cross Join, unconditional joins etc..
 * Contains a table index mapped with table name
 */
class DuckASTJoin : public DuckASTBaseOperator {
public:
	// Contains all table names
	vector<string> tables;

	// To be used with conditional joins
	// No requirement in cross joins with/without filter
	string condition;

	void AddTable(string &table_name);
	void SetCondition(string &condition);
};

/*
 * DuckASTGet has a 1:1 relationship with the LogicalGet
 * in src/include/duckdb/planner/logical_get.h
 */
class DuckASTGet : public DuckASTBaseOperator {
public:
	// Stores the name of the table on which SEQ_SCAN is being carried out
	std::string table_name;
	// Maps column_name to alias; if no alias leaves empty
	std::vector<std::pair<string, string>> column_aliases;
	// If all columns are selected from the table
	bool all_columns;
	// Filter (for filter pushdown)
	std::string filter_condition;
	DuckASTGet();
	DuckASTGet(string &table_name);
	~DuckASTGet() override;
	// sets the table name variable - just a setter function
	void SetTableName(string &table_name);
};

class DuckASTInsert : public DuckASTBaseOperator {
public:
	// Insert nodes only have the table name
	std::string table_name;
	DuckASTInsert();
	DuckASTInsert(string &t);
	~DuckASTInsert() override;
	void SetTableName(string &t);
};

class DuckASTNode {
public:
	unique_ptr<DuckASTBaseOperator> opr; // Operator
	string name;
	vector<unique_ptr<DuckASTNode>> children;
	DuckASTOperatorType type;
	DuckASTNode();
	DuckASTNode(unique_ptr<DuckASTBaseOperator> opr, DuckASTOperatorType type);
	void SetExpression(unique_ptr<DuckASTBaseOperator> opr, DuckASTOperatorType type);
	void Insert(unique_ptr<DuckASTBaseOperator> expr, unique_ptr<DuckASTNode> &parent_node, string &name,
	            DuckASTOperatorType type);
	void PrintAST(unique_ptr<DuckASTNode> node, const string &prefix, bool is_last);
};

void GenerateString(const unique_ptr<DuckASTNode> &node, string &plan_string);
void GenerateString(const unique_ptr<DuckASTNode> &node, string &prefix_string, string &plan_string,
		    bool has_filter = false, int join_child_index = -1);
} // namespace duckdb

#endif
