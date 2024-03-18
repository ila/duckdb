#include "data_representation.hpp"

/*
 * Refer: https://gist.github.com/destrex271/b3693b54bce6035749228aa45709b0e1 for extra notes
 */


namespace duckdb {
	// DuckASTBaseExpression
	DuckASTBaseExpression::DuckASTBaseExpression() {
		Printer::Print("Base Expression");
	}

	DuckASTBaseExpression::DuckASTBaseExpression(string name) {
	    this->name = name;
    }

	DuckASTBaseExpression::~DuckASTBaseExpression() {
	}

	// DuckASTProjection
	DuckASTProjection::DuckASTProjection() {
	}

	DuckASTProjection::DuckASTProjection(string name) {
		this->name = name;
    }

	DuckASTProjection::~DuckASTProjection() {
	}

	void DuckASTProjection::add_column(string table_index, string column_index, string alias) {
		this->column_alaises[table_index + "." + column_index] = alias;
	}

	// DuckASTFilter
	DuckASTFilter::DuckASTFilter() {
		this->filter_condition = "";
	}

	DuckASTFilter::DuckASTFilter(const string &filter_condition) {
		this->filter_condition = filter_condition;
	}

	void DuckASTFilter::set_filter_condition(string filter_condition) {
		this->filter_condition = filter_condition;
	}

	// DuckASTGet
	DuckASTGet::DuckASTGet() {
		this->all_columns = false;
	}

	DuckASTGet::DuckASTGet(string table_name, unsigned long int table_index) {
		this->table_name = table_name;
		this->table_index = table_index;
		this->all_columns = false;
	}

	void DuckASTGet::set_table_name(string table_name) {
		this->table_name = table_name;
	}

	DuckASTGet::~DuckASTGet() {
	}

	// DuckASTNode
	DuckASTNode::DuckASTNode() {
		this->expr = nullptr;
		this->type = DuckASTExpressionType::NONE;
    }

	DuckASTNode::DuckASTNode(shared_ptr<DuckASTBaseExpression> expr, DuckASTExpressionType type) {
		this->expr = expr;
		this->type = type;
		this->id = expr->name;
    }

	void DuckASTNode::setExpression(shared_ptr<DuckASTBaseExpression> expr, DuckASTExpressionType type) {
		this->expr = std::move(expr);
		this->type = type;
		this->id = expr->name;
    }

	// DuckAST
	DuckAST::DuckAST() {
		root = nullptr;
    }

	bool DuckAST::insert_after_root(shared_ptr<DuckASTNode> node, string parent_id, shared_ptr<DuckASTNode> curNode) {
		if(curNode->id == parent_id) {
			curNode->children.push_back(node);
			return true;
		}
		for(auto child: curNode->children) {
			bool result = insert_after_root(node, parent_id, child);
			if(result) return true;
		}
		return false;
    }


	void DuckAST::insert(shared_ptr<DuckASTBaseExpression>& expr, string id, DuckASTExpressionType type, string parent_id) {
		Printer::Print("Inserting: "+id);
		expr->name = id;
		if(root == nullptr) {
			root = (shared_ptr<DuckASTNode>)(new DuckASTNode(expr, type));
			root->type = type;
			root->parent_node = nullptr;
			Printer::Print("At root right now");
			return;
		}
		// todo: Add insert logic
		auto node = (shared_ptr<DuckASTNode>)(new DuckASTNode(expr, type));
		bool result = insert_after_root(node, parent_id, root);
		if(result) {
			Printer::Print("Inserted Element successfully");
		}else {
			Printer::Print("Unable to find element");
		}
    }

	void DuckAST::displayTree_t(shared_ptr<DuckASTNode> node) {
	    if(node == nullptr) return;
		Printer::Print(node->id);

		// Cast according to Nde Type
		switch(node->type) {
			case DuckASTExpressionType::FILTER: {
				auto exp = dynamic_cast<DuckASTFilter *>(node->expr.get());
				Printer::Print("Where " + exp->filter_condition);
				break;
			}
			case DuckASTExpressionType::GET: {
				auto exp = dynamic_cast<DuckASTGet *>(node->expr.get());
				Printer::Print(exp->table_name);
				Printer::Print("Columns IN GET: ");
				for(auto col: exp->alias_map) {
					Printer::Print(col.first + " as " + col.second);
				}
				break;
			}
			case DuckASTExpressionType::PROJECTION: {
				auto exp = dynamic_cast<DuckASTProjection*>(node->expr.get());
				Printer::Print("Columns to select in PROJJ:");
				for(auto col: exp->column_alaises) {
					Printer::Print(col.first + "+" + col.second);
				}
				break;
			}
		}

		for(auto child: node->children) {
			displayTree_t(child);
		}
    }

	void DuckAST::displayTree() {
	    if(root == nullptr) return;
		this->displayTree_t(root);
    }

	void DuckAST::generateString_t(shared_ptr<DuckASTNode> node, string &plan_string) {
		if(node == nullptr) return;

		// Append to plan_string according to node type
		switch(node->type) {
			case DuckASTExpressionType::PROJECTION: {
				for(auto child: node->children) {
					generateString_t(child, plan_string);
				}
				break;
			}
			case DuckASTExpressionType::FILTER: {
				auto exp = dynamic_cast<DuckASTFilter *>(node->expr.get());
				string condition = " where " + exp->filter_condition;
				plan_string = plan_string + condition;
				auto children = node->children;
				for(auto child: node->children) {
					generateString_t(child, plan_string);
				}
				break;
			}
			case DuckASTExpressionType::GET: {
				vector<string> columns;
				auto exp = dynamic_cast<DuckASTGet *>(node->expr.get());
				string table_name = exp->table_name;
				if(exp->all_columns) {
					plan_string = "select * from " + table_name + " " + plan_string;
					return;
				}
				for(auto col: exp->alias_map) {
					if(col.second == "") {
						columns.push_back(col.first);
					}else {
						columns.push_back(col.first + " as " + col.second);
					}
				}
				string cur_string = "select ";
				for(int i = 0; i < columns.size(); i++) {
					cur_string += columns[i];
					if(i != columns.size() - 1) {
						cur_string += ", ";
					}
				}
				cur_string += " from " + table_name;
				plan_string = cur_string + plan_string;

				// Move ahead in the tree
				// for(auto child: node->children) {
				// 	generateString_t(child, plan_string);
				// }
				// break;

				// Assuming that this is the bottom of the tree
				return;
			}
		}
    }


	void DuckAST::generateString(string &plan_string) {
		if(root == nullptr) return;
		this->generateString_t(root, plan_string);
		plan_string += ";";
    }


	// DuckASTNode DuckAST::get_node(string node_id, shared_ptr<DuckASTNode> node=nullptr) {
 //    }


}