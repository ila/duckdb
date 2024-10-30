#ifndef DUCKDB_OPENIVM_INSERT_RULE_HPP
#define DUCKDB_OPENIVM_INSERT_RULE_HPP

#include "../../compiler/include/logical_plan_to_string.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/function/table/read_csv.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
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
		optimizer_info = make_shared_ptr<IVMInsertOptimizerInfo>(false);
	}

	struct IVMInsertOptimizerInfo : OptimizerExtensionInfo {
		bool performed = false;
		// this flag is used to prevent the rule from being triggered twice
		// the plan is planned twice - when preparing and executing
		// the first time the flag is false, then we set it true after appending
		// if we find a true flag, we reset it as false for future insertions
		explicit IVMInsertOptimizerInfo(bool insertion_performed) : performed(insertion_performed) {
		}
	};

	static void IVMInsertRuleFunction(OptimizerExtensionInput &input,
	                                  duckdb::unique_ptr<LogicalOperator> &plan) {
		// first function call
		// we need to trigger this every time we see INSERT/DELETE/UPDATE on a table with IVM enabled
		if (plan->children.empty()) {
			return;
		}

		auto root = plan.get();
		if (root->GetName().substr(0, 6) != "INSERT" && root->GetName().substr(0, 6) != "DELETE" &&
		    root->GetName().substr(0, 6) != "UPDATE") {
			return;
		}

		switch (root->type) {
		case LogicalOperatorType::LOGICAL_INSERT: {

			// TODO - insert with query
			// todo - test with DEFAULT
			// todo - INSERT OR REPLACE INTO

			auto insert_node = dynamic_cast<LogicalInsert *>(root);
			// we need to check whether the table isn't the delta table already
			auto insert_table_name = insert_node->table.name;

			if (insert_table_name.substr(0, 6) == "delta_" || insert_table_name.empty()) {
				// this happens when we insert into the delta table (we don't want to insert twice)
				return;
			} else {
				auto insert_table = "delta_" + insert_node->table.name;
				QueryErrorContext error_context = QueryErrorContext();
				auto delta_table_catalog_entry = Catalog::GetEntry(
				    input.context, CatalogType::TABLE_ENTRY, insert_node->table.catalog.GetName(),
				    insert_node->table.schema.name, insert_table, OnEntryNotFound::RETURN_NULL, error_context);

				if (delta_table_catalog_entry) { // if it exists, we can append
					                             // check if already done

					// we also need to check whether the table is not a delta view
					// views are stored internally as tables -- example: view V on a table T
					// both delta_V and delta_T exist, we don't want insertions in V to be propagated to delta_V
					// we have metadata tables: _duckdb_ivm_views (view_name varchar primary key)

					Connection con(*input.context.db);
					con.SetAutoCommit(false);
					auto t = con.Query("select * from _duckdb_ivm_views where view_name = '" + insert_table_name + "'");
					if (t->RowCount() == 0) {
						// a view does not exist --> our table is actually a table and not a materialized view
						// we need a flag otherwise the insertion is performed twice (bug?)
						if (!dynamic_cast<IVMInsertOptimizerInfo *>(input.info.get())->performed) {
							// we need to reach the bottom of the tree to get the values to insert
							// insertion trees consist of: INSERT, PROJECTION, EXPRESSION_GET and a DUMMY_SCAN
							// we do not consider more complicated queries for the time being

							// we can have several types of INSERT queries
							// the simpler one is INSERT INTO table VALUES (v1, v2, ...)
							// consists in an INSERT node followed by PROJECTION
							// for COPY, we have INSERT followed by READ_CSV
							string insert_query;
							if (insert_node->children[0]->type == LogicalOperatorType::LOGICAL_PROJECTION) {

								insert_query = "insert into delta_" + insert_node->table.name + " values ";

								// COPY can just be another COPY in the delta table
								auto projection = dynamic_cast<LogicalProjection *>(insert_node->children[0].get());
								auto expression_get =
								    dynamic_cast<LogicalExpressionGet *>(projection->children[0].get());
								for (auto &expression : expression_get->expressions) {
									// each expression corresponds to a row
									// we build the query appending the values between parentheses
									// we need to check the type of the expression too (for VARCHAR fields)
									string values = "(";
									for (auto &value : expression) {
										if (value->type == ExpressionType::VALUE_CONSTANT) {
											auto constant = dynamic_cast<BoundConstantExpression *>(value.get());
											if (constant->value.type() == LogicalType::VARCHAR ||
											    constant->value.type() == LogicalType::DATE ||
											    constant->value.type() == LogicalType::TIMESTAMP ||
											    constant->value.type() == LogicalType::TIME) {
												values += "'" + constant->value.ToString() + "',";
											} else {
												values += constant->value.ToString() + ",";
											}
										} else {
											throw NotImplementedException("Only constant values are supported for now");
										}
									}
									// add "true" as multiplicity (we are performing an insertion)
									values += "true, now()),";
									insert_query += values;
								}

								// remove the last comma
								insert_query.pop_back();
								auto r = con.Query(insert_query);
								if (r->HasError()) {
									throw InternalException("Cannot insert in delta table! " + r->GetError());
								}
								con.Commit();
								dynamic_cast<IVMInsertOptimizerInfo *>(input.info.get())->performed = true;

							} else if (insert_node->children[0]->type == LogicalOperatorType::LOGICAL_GET) {
								// this is a COPY
								auto get = dynamic_cast<LogicalGet *>(insert_node->children[0].get());
								auto files = dynamic_cast<ReadCSVData *>(get->bind_data.get())->files; // vector
								for (auto &file : files) {
									// we cannot just copy; we need to hardcode the multiplicity and timestamp
									// insert into delta_table select *, true, now() from read_csv(path);
									// the performance is the same, since COPY is an insertion
									auto query = "insert into delta_" + insert_node->table.name + " select *, true, now() from read_csv('" + file + "');";
									auto r = con.Query(query);
									if (r->HasError()) {
										throw InternalException("Cannot insert in delta table! " + r->GetError());
									}
								}
								con.Commit();
								dynamic_cast<IVMInsertOptimizerInfo *>(input.info.get())->performed = true;
							}
						} else {
							// we skip the second insertion and reset the flag
							dynamic_cast<IVMInsertOptimizerInfo *>(input.info.get())->performed = false;
						}
					}
				}
			}
		} break;

		case LogicalOperatorType::LOGICAL_DELETE: {
			// delete plans consists in delete + filter + scan
			auto delete_node = dynamic_cast<LogicalDelete *>(root);
			// we need to check whether the table isn't the delta table already
			auto delete_table_name = delete_node->table.name;
			if (delete_table_name.substr(0, 6) == "delta_") {
				return;
			} else {
				auto delta_delete_table = "delta_" + delete_node->table.name;
				QueryErrorContext error_context = QueryErrorContext();
				auto delta_table_catalog_entry = Catalog::GetEntry(
				    input.context, CatalogType::TABLE_ENTRY, delete_node->table.catalog.GetName(),
				    delete_node->table.schema.name, delta_delete_table, OnEntryNotFound::RETURN_NULL, error_context);

				if (delta_table_catalog_entry) { // if it exists, we can append
					                             // check if already done
					Connection con(*input.context.db);
					con.SetAutoCommit(false);
					auto t = con.Query("select * from _duckdb_ivm_views where view_name = '" + delete_table_name + "'");
					if (t->RowCount() == 0) {
						if (!dynamic_cast<IVMInsertOptimizerInfo *>(input.info.get())->performed) {
							// I could not find any smart way to do this other than simply manipulating the string
							// we don't have the string, so we need to reconstruct it from the plan
							// todo 1 -- can there be other types of delete query?
							// todo 2 -- implement this in LPTS
							// todo 3 -- throw exception if the plan is too complicated
							string insert_string = "insert into " + delta_delete_table + " select *, false, now() from " +
							              delete_table_name;
							// handling the potential filters
							if (plan->children[0]->type == LogicalOperatorType::LOGICAL_FILTER) {
								auto filter = dynamic_cast<LogicalFilter *>(plan->children[0].get());
								insert_string += " where " + filter->ToString();
							} else if (plan->children[0]->type == LogicalOperatorType::LOGICAL_GET) {
								auto get = dynamic_cast<LogicalGet *>(plan->children[0].get());
								// we only add WHERE if there are table filters
								if (!get->table_filters.filters.empty()) {
									insert_string += " where " + get->ToString();
									insert_string = insert_string.substr(0, insert_string.find('\n'));
								}
							} else if (plan->children[0]->type == LogicalOperatorType::LOGICAL_EMPTY_RESULT) {
								// do nothing?
								// this might cause bugs but will be fixed as soon as we implement coalescing
								return;
							}
							else {
								throw NotImplementedException("Only simple DELETE statements are supported in IVM!");
							}

							auto r = con.Query(insert_string);
							if (r->HasError()) {
								throw InternalException("Cannot insert in delta table! " + r->GetError());
							}
							con.Commit();
							dynamic_cast<IVMInsertOptimizerInfo *>(input.info.get())->performed = true;
						} else {
							// we skip the second insertion and reset the flag
							dynamic_cast<IVMInsertOptimizerInfo *>(input.info.get())->performed = false;
						}
					}
				}
			}
		} break;

		case LogicalOperatorType::LOGICAL_UPDATE: {
			// updates consist in update + projection (+ filter) + scan
			auto update_node = dynamic_cast<LogicalUpdate *>(root);
			auto update_table_name = update_node->table.name;

			if (update_table_name.substr(0, 6) == "delta_") {
				return;
			} else {
				auto delta_update_table = "delta_" + update_node->table.name;
				QueryErrorContext error_context = QueryErrorContext();
				auto delta_table_catalog_entry = Catalog::GetEntry(
				    input.context, CatalogType::TABLE_ENTRY, update_node->table.catalog.GetName(),
				    update_node->table.schema.name, delta_update_table, OnEntryNotFound::RETURN_NULL, error_context);

				if (delta_table_catalog_entry) { // if it exists, we can append
					                             // check if already done
					Connection con(*input.context.db);
					con.SetAutoCommit(false);
					auto t = con.Query("select * from _duckdb_ivm_views where view_name = '" + update_table_name + "'");
					if (t->RowCount() == 0) {
						if (!dynamic_cast<IVMInsertOptimizerInfo *>(input.info.get())->performed) {
							// here we also assume simple queries with at most a filter
							// this is for the rows to delete
							string insert_old = "insert into " + delta_update_table + " select *, false, now() from " +
							                    update_table_name;
							// this is for the new rows to be added
							string insert_new = "insert into " + delta_update_table + " ";
							// we assume a projection with either a filter or a scan
							auto projection = dynamic_cast<LogicalProjection *>(update_node->children[0].get());

							// we want to extract the columns in the SET clause (there can be one or multiple)
							// these are stored in the expression of the projection
							// their structure is: value1, column1, value2, column2, ...
							// for convenience, we store this in a map of strings (to find the columns later)
							std::map<string, string> update_values;
							string where_string;
							// updates work this way: first, there is a UPDATE node whose columns
							// correspond to the physical table columns, in the order the query is issued
							// then, there is a PROJECTION node whose expressions are the constant values to be set
							// there may be more columns in the PROJECTION node, but we don't need those
							// so, we assume that UPDATE->columns[i] is to be set PROJECTION->expressions[i]
							for (size_t i = 0; i < update_node->columns.size(); i++) {
								auto column = update_node->columns[i].index;
								auto value = dynamic_cast<BoundConstantExpression *>(projection->expressions[i].get());
								// we need to add apostrophes for VARCHAR fields
								if (value->value.type() == LogicalType::VARCHAR ||
								    value->value.type() == LogicalType::DATE ||
								    value->value.type() == LogicalType::TIMESTAMP ||
								    value->value.type() == LogicalType::TIME) {
									update_values[to_string(column)] = "'" + value->value.ToString() + "'";
								} else {
									update_values[to_string(column)] = value->value.ToString();
								}
							}

							if (projection->children[0]->type == LogicalOperatorType::LOGICAL_FILTER) {
								auto filter = dynamic_cast<LogicalFilter *>(projection->children[0].get());
								// we always add WHERE (it's a filter, duh)
								where_string += " where " + filter->ToString();
							} else if (projection->children[0]->type == LogicalOperatorType::LOGICAL_GET) {
								auto get = dynamic_cast<LogicalGet *>(projection->children[0].get());
								// we only add WHERE if there are table filters
								if (!get->table_filters.filters.empty()) {
									where_string += " where " + get->ToString();
									where_string = where_string.substr(0, where_string.find('\n'));
								}
							} else if (plan->children[0]->type == LogicalOperatorType::LOGICAL_EMPTY_RESULT) {
								// do nothing
								// this might cause bugs but will be fixed as soon as we implement coalescing
								return;
							} else {
								throw NotImplementedException("Only simple UPDATE statements are supported in IVM!");
							}

							// now we build the second insert query
							insert_new += "select ";
							// if a column is not in the update_values map, we just copy its name
							// if it is in the map, we copy the value
							auto columns = update_node->table.GetColumns().GetColumnNames();
							for (size_t i = 0; i < columns.size(); i++) {
								if (update_values.find(to_string(i)) != update_values.end()) {
									insert_new += update_values[to_string(i)] + ", ";
								} else {
									insert_new += columns[i] + ", ";
								}
							}
							// we add the multiplicity flag
							insert_new += "true, now() from " + update_table_name + where_string;
							insert_old += where_string;

							auto r = con.Query(insert_old);
							if (r->HasError()) {
								throw InternalException("Cannot insert in delta table! " + r->GetError());
							}

							r = con.Query(insert_new);
							if (r->HasError()) {
								throw InternalException("Cannot insert in delta table! " + r->GetError());
							}

							con.Commit();
							dynamic_cast<IVMInsertOptimizerInfo *>(input.info.get())->performed = true;
						} else {
							// we skip the second insertion and reset the flag
							dynamic_cast<IVMInsertOptimizerInfo *>(input.info.get())->performed = false;
						}
					}
				}
			}
		} break;
		default:
			return;
		}
	}
};
}; // namespace duckdb

#endif // DUCKDB_OPENIVM_INSERT_RULE_HPP
