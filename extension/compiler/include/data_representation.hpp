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
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/planner/tableref/bound_basetableref.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/common/printer.hpp"

// Naming the representation: DuckAST

namespace duckdb {

// DuckASTBaseOperator Types
enum DuckASTOperatorType { NONE, PROJECTION, GET, FILTER, AGGREGATE, ORDER_BY, INSERT };

// Node Type Classes
class DuckASTBaseOperator {
	// Rename: Operator
public:
	string name;
	DuckASTBaseOperator();
	DuckASTBaseOperator(string name);
	virtual ~DuckASTBaseOperator();
};

class DuckASTProjection : public DuckASTBaseOperator {
public:
	std::map<string, string> column_aliases;
	DuckASTProjection();
	DuckASTProjection(string name);
	~DuckASTProjection() override;
	void add_column(string table_index, string column_index, string alias);
};

class DuckASTFilter : public DuckASTBaseOperator {
public:
	std::string filter_condition;
	DuckASTFilter();
	DuckASTFilter(const string &filter_condition);
	void set_filter_condition(string filter_condition);
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

class DuckASTCrossJoin: public DuckASTBaseOperator {
public:
	map<int, string> tables;
	DuckASTCrossJoin();
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
	void add_order_column(string &col, string &ord);
};

/*
 * Will support all the joins like Cross Join, unconditional joins etc..
 */
class DuckASTJoin: public DuckASTBaseOperator {
public:
	// Maps table index to table name to generate the 'from' part of the query
	map<int, string> table_map;

	// To be used with conditional joins
	// No requirement in cross joins with/without filter
	string condition;
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
	DuckASTGet();
	DuckASTGet(string table_name);
	~DuckASTGet() override;
	// sets the table name variable - just a setter function
	void set_table_name(string table_name);
};

class DuckASTInsert : public DuckASTBaseOperator {
public:
	// Insert nodes only have the table name
	std::string table_name;
	DuckASTInsert();
	DuckASTInsert(string t);
	~DuckASTInsert() override;
	void set_table_name(string t);
};

class DuckASTNode {
public:
	shared_ptr<DuckASTBaseOperator> opr; // Operator
	string name;
	vector<shared_ptr<DuckASTNode>> children;
	shared_ptr<DuckASTNode> parent_node;
	DuckASTOperatorType type;
	DuckASTNode();
	DuckASTNode(shared_ptr<DuckASTBaseOperator> opr, DuckASTOperatorType type);
	void setExpression(shared_ptr<DuckASTBaseOperator> opr, DuckASTOperatorType type);
};
class DuckAST {
private:
	void displayTree(shared_ptr<DuckASTNode> node);
	void generateString(shared_ptr<DuckASTNode> node, string &prefix_string, string &plan_string,
	                      bool has_filter = false);
	shared_ptr<DuckASTNode> last_ptr = nullptr;

public:
	DuckAST();
	void insert(shared_ptr<DuckASTBaseOperator> &expr, shared_ptr<DuckASTNode> &parent_node, string name, DuckASTOperatorType type);
	static void printAST(shared_ptr<duckdb::DuckASTNode> node, string prefix = "", bool isLast = true);
	void generateString(string &plan_string);
	void printAST(shared_ptr<duckdb::DuckAST> ast);
	shared_ptr<DuckASTNode> getLastNode();
	shared_ptr<DuckASTNode> root;
};
} // namespace duckdb

#endif
