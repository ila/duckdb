#ifndef DUCKDB_OPENIVM_INSERT_RULE_HPP
#define DUCKDB_OPENIVM_INSERT_RULE_HPP

#include "../../compiler/include/logical_plan_to_string.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/planner/operator/logical_expression_get.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/planner/tableref/bound_basetableref.hpp"
#include "openivm_parser.hpp"

#include <iostream>
#include <utility>

namespace duckdb {

class IVMInsertRule : public OptimizerExtension {
public:
	IVMInsertRule() {
		optimize_function = IVMInsertRuleFunction;
		optimizer_info = make_shared<IVMInsertOptimizerInfo>(false);
	}

	struct IVMInsertOptimizerInfo : OptimizerExtensionInfo {
		bool insertion_performed = false;
		// this flag is used to prevent the rule from being triggered twice
		// the plan is planned twice - when preparing and executing
		// the first time the flag is false, then we set it true after appending
		// if we find a true flag, we reset it as false for future insertions
		explicit IVMInsertOptimizerInfo(bool insertion_performed) : insertion_performed(insertion_performed) {
		}
	};

	static void IVMInsertRuleFunction(ClientContext &context, OptimizerExtensionInfo *info,
	                                  duckdb::unique_ptr<LogicalOperator> &plan) {
		// first function call
		// we need to trigger this every time we see INSERT/DELETE/UPDATE on a table with IVM enabled
		if (plan->children.empty()) {
			return;
		}

		auto root = plan.get();
		if (root->GetName().substr(0, 6) != "INSERT" && root->GetName().substr(0, 6) != "DELETE" &&
		    root->GetName().substr(0, 6) != "UPDATE") {
			// todo maybe this can be more elegant
			return;
		}

#ifdef DEBUG
		printf("Activating the IVM insert rule\n");
#endif

		switch (root->type) {
		case LogicalOperatorType::LOGICAL_INSERT: {

			auto insert_node = dynamic_cast<LogicalInsert *>(root);
			// we need to check whether the table isn't the delta table already
			auto insert_table_name = insert_node->table.name;

			// we also need to check whether the table is not a delta view
			// todo

			if (insert_table_name.substr(0, 6) == "delta_" || insert_table_name.empty()) {
				// this happens when we insert into the delta table (we don't want to insert twice)
				return;
			} else {
				auto insert_table = "delta_" + insert_node->table.name;
				QueryErrorContext error_context = QueryErrorContext();
				auto delta_table_catalog_entry = Catalog::GetEntry(
				    context, CatalogType::TABLE_ENTRY, insert_node->table.catalog.GetName(),
				    insert_node->table.schema.name, insert_table, OnEntryNotFound::RETURN_NULL, error_context);

				if (delta_table_catalog_entry) { // if it exists, we can append
					                             // check if already done
					std::cout << plan->ToString();
					// todo -- check why we need a flag (I forgot)
					// todo monday -- this is called also when we are reinserting stuff in the delta view
					if (!dynamic_cast<IVMInsertOptimizerInfo *>(info)->insertion_performed) {

						// we need to reach the bottom of the tree to get the values to insert
						// insertion trees consist of: INSERT, PROJECTION, EXPRESSION_GET and a DUMMY_SCAN
						// we do not consider more complicated queries for the time being

						auto insert_query = "insert into delta_" + insert_node->table.name + " values ";

						// todo -- handle the case of COPY, bulk insertion etc
						auto projection = dynamic_cast<LogicalProjection *>(insert_node->children[0].get());
						auto expression_get = dynamic_cast<LogicalExpressionGet *>(projection->children[0].get());
						for (auto &expression : expression_get->expressions) {
							// each expression corresponds to a row
							// we build the query appending the values between parentheses
							// we need to check the type of the expression too (for VARCHAR fields)
							string values = "(";
							for (auto &value : expression) {
								if (value->type == ExpressionType::VALUE_CONSTANT) {
									auto constant = dynamic_cast<BoundConstantExpression *>(value.get());
									if (constant->value.type() == LogicalType::VARCHAR || constant->value.type() == LogicalType::DATE ||
									    constant->value.type() == LogicalType::TIMESTAMP || constant->value.type() == LogicalType::TIME) {
										values += "'" + constant->value.ToString() + "',";
									} else {
										values += constant->value.ToString() + ",";
									}
								} else {
									throw NotImplementedException("Only constant values are supported for now");
								}
							}
							// add "true" as multiplicity (we are performing an insertion)
							values += "true),";
							insert_query += values;
						}

						// remove the last comma
						insert_query.pop_back();

						Connection con(*context.db);
						con.SetAutoCommit(false);
						// todo exception handling
						auto r = con.Query(insert_query);
						con.Commit();

						dynamic_cast<IVMInsertOptimizerInfo *>(info)->insertion_performed = true;
					} else {
						// we skip the second insertion and reset the flag
						dynamic_cast<IVMInsertOptimizerInfo *>(info)->insertion_performed = false;
					}
					return;
				}
			}
		}
		case LogicalOperatorType::LOGICAL_DELETE: {
			// delete plans consists in delete + filter + scan
			/*
			auto delete_node = dynamic_cast<LogicalDelete *>(root);
			// we need to check whether the table isn't the delta table already
			auto delete_table_name = delete_node->table.name;
			if (delete_table_name.substr(0, 6) == "delta_") {
			    // todo - throw an exception?
			    return;
			} else {
			    // delete_node->return_chunk = true;
			    auto delete_table = "delta_" + delete_node->table.name;
			    QueryErrorContext error_context = QueryErrorContext();
			    auto delta_table_catalog_entry = Catalog::GetEntry(
			        context, CatalogType::TABLE_ENTRY, delete_node->table.catalog.GetName(),
			        delete_node->table.schema.name, delete_table, OnEntryNotFound::RETURN_NULL, error_context);

			    if (delta_table_catalog_entry) { // if it exists, we can append
			        // check if already done
			        if (!dynamic_cast<IVMInsertOptimizerInfo *>(info)->insertion_performed) {
			            auto delta_table_entry = dynamic_cast<TableCatalogEntry *>(delta_table_catalog_entry.get());

			            Connection con(*context.db);
			            InternalAppender appender(context, *delta_table_entry);

			        } else {
			            // we skip the second insertion and reset the flag
			            dynamic_cast<IVMInsertOptimizerInfo *>(info)->insertion_performed = false;
			        }
			        return;
			    }
			} */
		}
		case LogicalOperatorType::LOGICAL_UPDATE: {
			// updates consist in update + projection (+ filter) + scan
			// auto update_node = dynamic_cast<LogicalUpdate *>(root);
		}
		default:
			return;
		}
	}
};
}; // namespace duckdb

#endif // DUCKDB_OPENIVM_INSERT_RULE_HPP
