#include "rdda_parser.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/planner.hpp"

#include <iostream>
#include <stack>

namespace duckdb {

string RDDAParserExtension::path;
string RDDAParserExtension::db;

ParserExtensionParseResult RDDAParserExtension::RDDAParseFunction(ParserExtensionInfo *info, const string &query) {
	// very rudimentary parser trying to find RDDA statements
	// the query is parsed twice, so we expect that any SQL mistakes are caught in the second iteration
	// firstly, we try to understand whether this is a SELECT/ALTER/CREATE/DROP/... expression

	// auto query_lower = RDDALowerCase(StringUtil::Replace(query, "\n", ""));
	auto query_lower = StringUtil::Lower(StringUtil::Replace(query, "\n", ""));
	StringUtil::Trim(query_lower);

	// each instruction set gets saved to a file, for portability
	string parser_query; // the SQL-compliant query to be fed to the parser
	string centralized_queries = "";
	string decentralized_queries = "";
	string third_party_queries = "";

	// reading config from settings table
	string db_name = path + db;
	string parser_db_name = path + "parser.db";
	DuckDB settings_db(db_name);
	Connection settings_con(settings_db);

	// creating a backup connection such that we can check for syntax errors
	// this stores all the definitions in a file, such that even at a future moment we have a permanent snapshot
	DuckDB parser_db(parser_db_name);
	Connection parser_con(parser_db);

	if (query_lower.substr(0, 6) == "create") {
		// this is a CREATE statement
		// splitting the statement to extract column names and constraints
		// the table name is the last string that happens before the first parenthesis
		auto scope = ParseScope(query_lower);
		// check if this is a CREATE table or view
		if (query_lower.substr(0, 12) == "create table") {
			// remove RDDA constraints from the string
			// todo can constraints be empty?
			std::vector<rdda_constraint> constraints;
			if (scope == table_scope::decentralized) {
				constraints = ParseCreateTable(query_lower);
			}
			// query is clean now, let's try and feed it back to the parser
			std::cout << "parsing: " << query_lower << "\n";
			auto result = parser_con.Query(query_lower);
			if (!result->HasError()) {
				// query succeeded, now we translate our RDDA keywords into SQL statements
				auto table_name = ExtractTableName(query_lower);
				std::cout << table_name << "\n";
				// now we add the table in our RDDA catalog (name, scope, query)
				auto table_string = "insert into rdda_tables values(" + table_name + ", " +
				                    std::to_string(static_cast<int32_t>(scope)) + ", NULL);\n";
				centralized_queries += table_string;
				query_lower += "\n";
				if (scope == table_scope::decentralized) {
					decentralized_queries += query_lower;
					if (!constraints.empty()) {
						for (auto &constraint : constraints) {
							// we assume that the system table already exists
							if (constraint.minimum_aggregation > 0 || constraint.randomized || constraint.sensitive) {
								auto constraint_string = "insert into rdda_table_constraints values ('" + table_name +
								                         "', '" + constraint.column_name + "', " +
								                         std::to_string(static_cast<uint8_t>(constraint.randomized)) +
								                         ", " +
								                         std::to_string(static_cast<uint8_t>(constraint.sensitive)) +
								                         ", " + std::to_string(constraint.minimum_aggregation) + ");\n";
								centralized_queries += constraint_string;
							}
						}
					}
				} else {
					if (scope == table_scope::replicated) {
						centralized_queries += query_lower;
						decentralized_queries += query_lower;
					} else {
						// centralized table
						centralized_queries += query_lower;
					}
				}
			} else {
				throw ParserException(result->GetError());
			}
		} else if (query_lower.substr(0, 24) == "create materialized view") {
			// we have a view, create instructions to create and propagate it within the system
			// "materialized view" is replaced by "table" here

			// note: the table/view is only created on endpoints
			// central/third party servers can read from csv
			// we still need to check constraints

			auto view_constraints = ParseCreateView(query_lower);

			// ttl must be defined on a column with granularity
			// todo

			parser_con.Query("drop table user_run_daily;"); // DEBUG
			auto result = parser_con.Query(query_lower);
			if (!result->HasError()) {
				// adding the view to the system tables
				auto view_name = ExtractTableName(query_lower);
				std::cout << "\nname: " << view_name << "\n";
				auto view_query = ExtractViewQuery(query_lower);
				std::cout << "query: " << view_query << "\n";
				// extracting the FROM and JOIN clause tables to store the query scope
				auto view_tables = ParseViewTables(query_lower);
				std::cout << "tables: " << view_tables << "\n";

				parser_con.Query("SET disabled_optimizers TO 'expression_rewriter, statistics_propagation'");

				// parsing the logical plan
				parser_con.BeginTransaction();
				Parser p;
				p.ParseQuery(view_query);

				Planner planner(*parser_con.context);

				planner.CreatePlan(move(p.statements[0]));
				auto plan = move(planner.plan);

				Optimizer optimizer(*planner.binder, *parser_con.context);
				plan = optimizer.Optimize(move(plan));
				std::cout << plan->ToString();

				std::stack<LogicalOperator *> node_stack;
				node_stack.push(plan.get());

				while (!node_stack.empty()) {
					auto current = node_stack.top();
					node_stack.pop();

					std::cout << "Found node: " << LogicalOperatorToString(current->type) << "\n";
					std::cout << "Table index vector: ";
					auto idx = current->GetTableIndex();
					for (auto &i : idx)
						std::cout << i << " ";
					std::cout << "\n";
					for (auto &c : current->GetColumnBindings()) {
						std::cout << "table index column: " << c.table_index << " column index: " << c.column_index
						          << "\n";
					}
					std::cout << std::endl;

					std::cout << "Children:\n";

					if (!current->children.empty()) {
						// Push children onto the stack in reverse order to process them in a depth-first manner
						for (auto it = current->children.rbegin(); it != current->children.rend(); ++it) {
							node_stack.push(it->get());
						}
					}
				}

				// extract the column indexes and table indexes we want
				/*
				std::unordered_map<int32_t, std::set<int32_t>> columns;

				std::multiset<std::pair<int32_t, int32_t>> old_bindings;
				std::set<std::pair<int32_t, int32_t>> new_bindings;
				std::multiset<std::pair<int32_t, int32_t>> old_bindings_child;
				std::set<std::pair<int32_t, int32_t>> new_bindings_child;
				 */

				// again
				/*
				node_stack.push(plan.get());
				while (!node_stack.empty()) {
				    auto current = node_stack.top();
				    node_stack.pop();

				    for (auto &c : current->GetColumnBindings()) {
				        if (old_bindings.empty()) {
				            old_bindings.insert(std::make_pair(c.table_index, c.column_index));
				        } else {
				            old_bindings_child.insert(std::make_pair(c.table_index, c.column_index));
				        }
				    }
				    // new bindings
				    if (plan->type == LogicalOperatorType::LOGICAL_PROJECTION) {
				        for (auto &e : plan->expressions) {
				            if (e->type == ExpressionType::BOUND_COLUMN_REF) {
				                if (new_bindings.empty()) {
				                    new_bindings.insert(
				                        std::make_pair(e->Cast<BoundColumnRefExpression>().binding.table_index,
				                                       e->Cast<BoundColumnRefExpression>().binding.column_index));
				                } else {
				                    // we have a parent
				                    new_bindings_child.insert(
				                        std::make_pair(e->Cast<BoundColumnRefExpression>().binding.table_index,
				                                       e->Cast<BoundColumnRefExpression>().binding.column_index));
				                }
				            } else if (e->type == ExpressionType::BOUND_FUNCTION) {
				                for (auto &c : e->Cast<BoundFunctionExpression>().children) {
				                    if (c->type == ExpressionType::BOUND_COLUMN_REF) {
				                        new_bindings.insert(std::make_pair(c->Cast<BoundColumnRefExpression>().binding.table_index,
				c->Cast<BoundColumnRefExpression>().binding.column_index));

				                    }
				                }
				            } // todo all other use cases?
				        }
				    }

				    if (!old_bindings.empty() && !old_bindings_child.empty() && !new_bindings.empty() &&
				!new_bindings_child.empty()) {
				        // resolve bindings
				        // are the old bindings of the parent equal to the new ones of the child?

				    }


				    std::cout << std::endl;

				    std::cout << "Children:\n";

				    if (!current->children.empty()) {
				        // Push children onto the stack in reverse order to process them in a depth-first manner
				        for (auto it = current->children.rbegin(); it != current->children.rend(); ++it) {
				            node_stack.push(it->get());
				        }
				    }
				}

				*/

				/*
				while (!node_stack.empty()) {
				    auto current = node_stack.top();
				    node_stack.pop();

				    // tree root
				    if (columns.empty()) {
				        if (plan->type == LogicalOperatorType::LOGICAL_PROJECTION) {
				            for (auto &e : plan->expressions) {
				                if (e->type == ExpressionType::BOUND_COLUMN_REF) {
				                    columns[e->Cast<BoundColumnRefExpression>().binding.table_index].insert(
				                        e->Cast<BoundColumnRefExpression>().binding.column_index);
				                } else if (e->type == ExpressionType::BOUND_FUNCTION) {
				                    for (auto &c : e->Cast<BoundFunctionExpression>().children) {
				                        if (e->type == ExpressionType::BOUND_COLUMN_REF) {
				                            columns[e->Cast<BoundColumnRefExpression>().binding.table_index].insert(
				                                e->Cast<BoundColumnRefExpression>().binding.column_index);
				                        }
				                    }
				                } // todo all other use cases?
				            }
				        }
				    } else {
				        if (current->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
				            // check if there is a projection on top
				            if (current->children[0]->type == LogicalOperatorType::LOGICAL_PROJECTION) {
				                // first we find the new table index
				                if (!current->Cast<LogicalAggregate>().groups.empty()) {
				                    int32_t new_table_index = current->Cast<LogicalAggregate>()
				                                                  .groups[0]
				                                                  ->Cast<BoundColumnRefExpression>()
				                                                  .binding.table_index;
				                    // now we get aggregate indexes
				                    std::set<int32_t> aggregate_indexes;
				                    for (auto &e : current->expressions) {
				                        aggregate_indexes.insert(e->Cast<BoundAggregateExpression>()
				                                                     .children[0]
				                                                     ->Cast<BoundColumnRefExpression>()
				                                                     .binding.column_index);
				                    }
				                    auto group_index = current->Cast<LogicalAggregate>().group_index;
				                    auto aggregate_index = current->Cast<LogicalAggregate>().aggregate_index;
				                    if (columns.find(aggregate_index) != columns.end()) {
				                        for (auto &i : aggregate_indexes) {
				                            columns[new_table_index].insert(i);
				                        }
				                        // now checking the group indexes
				                        auto it = columns.find(group_index);
				                        if (it != columns.end()) {
				                            for (auto &i : current->Cast<LogicalAggregate>().groups) {
				                                for (auto &c : it->second) {
				                                    if (columns[new_table_index].find(c) ==
				columns[new_table_index].end()) { columns[new_table_index].insert(c); } else {
				                                        columns[new_table_index].insert(c + 1);
				                                    }
				                                }
				                            }
				                            columns.erase(current->Cast<LogicalAggregate>().group_index);
				                        }
				                        columns.erase(current->Cast<LogicalAggregate>().aggregate_index);
				                    }
				                }
				            }
				        }

				        // projection
				        // smth is wrong here
				        if (current->type == LogicalOperatorType::LOGICAL_PROJECTION) {
				            // we just update the index
				            auto table_index = current->Cast<LogicalProjection>().table_index;
				            for (auto &e : current->expressions) {
				                if (!columns[table_index].empty()) {
				                    int32_t new_table_index = current->Cast<LogicalProjection>()
				                                                  .expressions[0]
				                                                  ->Cast<BoundColumnRefExpression>()
				                                                  .binding.table_index;
				                    for (auto &i : current->Cast<LogicalProjection>().expressions) {
				                        auto column_index = i->Cast<BoundColumnRefExpression>().binding.column_index;
				                        if (columns[table_index].find(column_index) != columns[table_index].end()) {
				                            columns[new_table_index].insert(column_index);
				                        }
				                    }
				                }
				                columns.erase(table_index);
				            }
				        }
				    }
				    if (!current->children.empty()) {
				        // push children onto the stack in reverse order to process them in a depth-first manner
				        for (auto it = current->children.rbegin(); it != current->children.rend(); ++it) {
				            node_stack.push(it->get());
				        }
				    }
				} */

				/*
				 * projections can create new columns, so they get an index
				 * aggregates get 3 indexes: logical_aggregate.hpp - group, aggregate, groupings
				 * everything can create a new column gets a new index
				 * bind context is recursive
				 * for the physical projection, get all the expressions and traverse the tree
				 * now we trace down the binder joins
				 * expression iterator
				 * logical operator visitor
				 */

				// VisitOperator(p);

				auto view = parser_con.TableInfo(view_name);

				auto table_info = make_uniq<CreateTableInfo>();
				table_info->schema = view->schema;
				table_info->table = view->table + "_centralized";
				table_info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
				table_info->temporary = false;

				for (idx_t i = 0; i < view->columns.size(); i++) {
					table_info->columns.AddColumn(view->columns[i].Copy());
				}

				// adding two timestamp columns
				ColumnDefinition generation_timestamp("generation_timestamp", LogicalType::TIMESTAMP);
				ColumnDefinition arrival_timestamp("arrival_timestamp", LogicalType::TIMESTAMP);

				table_info->columns.AddColumn(move(generation_timestamp));
				table_info->columns.AddColumn(move(arrival_timestamp));

				// todo test this
				// todo replace this with the sql from duckdb_tables
				BufferedFileWriter serializer(parser_db.GetFileSystem(), path + "centralized_tables.binary");
				// table_info->Serialize(serializer);

				if (scope == table_scope::centralized) {
					// check if the query comes from decentralized tables
					// todo what to do with all of this?
					std::stringstream ss(view_tables);
					string table;

					while (getline(ss, table, ',')) {
						string query_check = "select * from rdda_tables where name = '" + table + "' and type = 2;";
						auto result_check = settings_con.Query(query_check);
						if (result_check->RowCount() == 0) {
							throw ParserException("Centralized view definition should come from decentralized tables!");
						}
					}
					// all good, we can elaborate the query
					// only add this once (we assume 1:1 mapping between centralized and decentralized views)
					auto view_string = "insert into rdda_tables values(" + view_name + ", " +
					                   std::to_string(static_cast<int32_t>(scope)) + ", " + view_query + ");\n";
					centralized_queries += view_string;
					// storing constraints (we assume that the system table already exists)
					auto constraint_string = "insert into rdda_view_constraints values ('" + view_name + "', '" +
					                         std::to_string(view_constraints.window) + "', " +
					                         std::to_string(view_constraints.ttl) + ");\n";
					// storing the underlying query
					auto view_tables_string = "insert into rdda_queries values ('" + view_name + "', '" + view_tables +
					                          "', '" + view_query + "');\n";
					// now we store the query client side too
					decentralized_queries += view_tables_string;
					// creating the centralized table to permanently store results
					// auto centralized_table_string = "create table rdda_centralized_table_" + view_name + " as select
					// * from "
					//                                + view_name + ";\n";
					// also creating the correspondent decentralized view on endpoints
					auto decentralized_view_query = "create view " + view_name + " as " + query_lower + "\n";
					decentralized_queries += decentralized_view_query;
					// creating the centralized view
					// auto centralized_view_query = "create view rdda_centralized_view_" + view_name + " as " +
					// query_lower
					// + "\n"; centralized_queries += centralized_view_query;
					centralized_queries += constraint_string;
					centralized_queries += view_tables_string;
					// centralized_queries += centralized_table_string;

					// last thing we do is creating a table to store encrypted results
					// todo change this
					/*
					auto encrypted_query = "create table rdda_centralized_view_encrypted_" + view_name +
					                       "(encrypted_block text, client_id bigint, arrival_time timestamp);\n";
					third_party_queries += encrypted_query; */

				} else if (scope == table_scope::decentralized) {
					throw ParserException("Decentralized views are generated automatically!");
				} else {
					// replicated
					// todo - establish semantics
					auto replicated_view_query = "create table " + view_name + " as " + query_lower + "\n";
					decentralized_queries += replicated_view_query;
					// todo change this table
					centralized_queries += replicated_view_query;
				}
			} else {
				throw ParserException(result->GetError());
			}
		}
	} else if (query_lower.substr(0, 7) == "select") {
		// this is a SELECT statement
		// auto option = ParseSelectQuery(query_lower);
		// todo
	} else if (query_lower.substr(0, 7) == "insert") {
		// this is a INSERT statement
		// todo
	} else if (query_lower.substr(0, 7) == "update") {
		// this is an UPDATE statement
		// todo
	} else if (query_lower.substr(0, 7) == "delete") {
		// this is a DELETE statement
		// todo
	} else if (query_lower.substr(0, 5) == "drop") {
		// this is a DROP statement
		// todo
	} else if (query_lower.substr(0, 11) == "alter table") {
		// this is an ALTER statement
		// todo
	}

	string path_centralized = path + "centralized_queries.sql";
	string path_decentralized = path + "decentralized_queries.sql";
	string path_third_party = path + "third_party_queries.sql";

	// writing the newly parsed SQL commands
	if (!centralized_queries.empty()) {
		WriteFile(centralized_queries, path_centralized);
	}
	if (!decentralized_queries.empty()) {
		WriteFile(decentralized_queries, path_decentralized);
	}
	if (!third_party_queries.empty()) {
		WriteFile(third_party_queries, path_third_party);
	}

	std::cout << "done!\n" << std::endl;

	// todo fix this
	return ParserExtensionParseResult("Test!");
}

std::string RDDAParserExtension::Name() {
	return "rdda_parser_extension";
}
}; // namespace duckdb
