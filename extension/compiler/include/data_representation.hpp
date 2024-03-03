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

  // DuckASTBaseExpression Types
  enum DuckASTExpressionType {
    NONE,
    PROJECTION,
    GET
  };

  // Node Type Classes
  class DuckASTBaseExpression {
    public:
      string name;
      DuckASTBaseExpression();
      DuckASTBaseExpression(string name);
      virtual ~DuckASTBaseExpression();
  };

  class DuckASTProjection : public DuckASTBaseExpression{
    public:
      std::map<string, string> column_alaises;
      DuckASTProjection();
      DuckASTProjection(string name);
      ~DuckASTProjection() override;
      void add_column(string table_index, string column_index, string alias);
  };

  class DuckASTGet : public DuckASTBaseExpression {
    public:
      std::string table_name;
      unsigned long int table_index;
      std::vector<string> column_names;
      bool all_columns;
      // todo: Column Index for bindings
      DuckASTGet();
      DuckASTGet(string table_name, unsigned long int table_index, std::vector<string> col_names);
      ~DuckASTGet() override;
      void set_table_name(string table_name);
      // void set_column_names(std::vector<string> column_names);
      // void add_column_name(string column_name);
  };
  // Filter PushDown
 // Separate Type for filter?

  // Expression Tree
  class DuckASTNode {
    public:
      shared_ptr<DuckASTBaseExpression> expr;
      string id;
      vector<shared_ptr<DuckASTNode>> children;
      shared_ptr<DuckASTNode> parent_node;
      DuckASTExpressionType type;
      DuckASTNode();
      DuckASTNode(shared_ptr<DuckASTBaseExpression> expr, DuckASTExpressionType type);
      void setExpression(shared_ptr<DuckASTBaseExpression> expr, DuckASTExpressionType type);
  };
  class DuckAST {
    private:
      shared_ptr<DuckASTNode> root;
      bool insert_after_root(shared_ptr<DuckASTNode> node, string parent_id, shared_ptr<DuckASTNode> curNode);
      void displayTree_t(shared_ptr<DuckASTNode> node);
    public:
      DuckAST();
      void insert(shared_ptr<DuckASTBaseExpression>& expr, string id, DuckASTExpressionType type, string parent_id);
      void displayTree();
  };
} // namespace duckdb

#endif