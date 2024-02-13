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

// Naming the representation: QuackQL

namespace duckdb {

  // QuackQLBaseExpression Types
  enum QuackQLExpressionType {
    NONE,
    PROJECTION
  };

  // Node Type Classes
  class QuackQLBaseExpression {
    public:
      string name;
      QuackQLBaseExpression();
      QuackQLBaseExpression(string name);
      ~QuackQLBaseExpression();
  };

  class QuackQLProjection : public QuackQLBaseExpression{
    public:
      std::vector<string> column_names;
      QuackQLProjection();
      QuackQLProjection(string name);
      ~QuackQLProjection();
  };

  // Expression Tree
  class QuackQLNode {
    public:
      shared_ptr<QuackQLBaseExpression> expr;
      vector<shared_ptr<QuackQLNode>> children;
      QuackQLExpressionType type;
      QuackQLNode();
      QuackQLNode(shared_ptr<QuackQLBaseExpression> expr, QuackQLExpressionType type);
      void setExpression(shared_ptr<QuackQLBaseExpression> expr, QuackQLExpressionType type);
  };
  class QuackQLTree {
    private:
      shared_ptr<QuackQLNode> root;
    public:
      QuackQLTree();
      void insert(shared_ptr<QuackQLBaseExpression>& expr, string id, QuackQLExpressionType type);
      void displayTree_t(shared_ptr<QuackQLNode> node);
      void displayTree();
  };
} // namespace duckdb

#endif