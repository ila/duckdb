#include "data_representation.hpp"

#include <iostream>

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
	this->column_aliases[table_index + "." + column_index] = alias;
}

// DuckASTAggregate
DuckASTAggregate::DuckASTAggregate(vector<string> &aggregate_function, vector<string> &group_column) {
	this->aggregate_function = aggregate_function;
	this->group_column = group_column;
	if (this->group_column.size() == 0) {
		this->is_group_by = false;
	} else {
		this->is_group_by = true;
	}
}

// DuckASTOrderBy
DuckASTOrderBy::DuckASTOrderBy() {
}

void DuckASTOrderBy::add_order_column(string &col, string &ord) {
	this->order[col] = ord;
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
	if (curNode->id == parent_id) {
		curNode->children.push_back(node);
		return true;
	}
	for (auto child : curNode->children) {
		bool result = insert_after_root(node, parent_id, child);
		if (result)
			return true;
	}
	return false;
}

void DuckAST::insert(shared_ptr<DuckASTBaseExpression> &expr, string id, DuckASTExpressionType type, string parent_id) {
	Printer::Print("Inserting: " + id);
	expr->name = id;
	if (root == nullptr) {
		root = (shared_ptr<DuckASTNode>)(new DuckASTNode(expr, type));
		root->type = type;
		root->parent_node = nullptr;
		Printer::Print("At root right now");
		return;
	}
	// todo: Add insert logic
	auto node = (shared_ptr<DuckASTNode>)(new DuckASTNode(expr, type));
	bool result = insert_after_root(node, parent_id, root);
	if (result) {
		Printer::Print("Inserted Element successfully");
	} else {
		Printer::Print("Unable to find element");
	}
}

void DuckAST::generateString_t(shared_ptr<DuckASTNode> node, string &plan_string, vector<string> &additional_cols,
                               bool has_filter) {
	if (node == nullptr)
		return;

	// Append to plan_string according to node type
	switch (node->type) {
	case DuckASTExpressionType::PROJECTION: {
		for (auto child : node->children) {
			generateString_t(child, plan_string, additional_cols);
		}
		break;
	}
	case DuckASTExpressionType::FILTER: {
		auto exp = dynamic_cast<DuckASTFilter *>(node->expr.get());
		string condition = exp->filter_condition;
		plan_string = plan_string + condition;
		auto children = node->children;
		for (auto child : node->children) {
			generateString_t(child, plan_string, additional_cols, true);
		}
		break;
	}
	case DuckASTExpressionType::ORDER_BY: {
		auto exp = dynamic_cast<DuckASTOrderBy *>(node->expr.get());
		string order_string = "";
		int cnt = exp->order.size();
		for (auto ord : exp->order) {
			order_string += ord.first + " " + ord.second;
			cnt--;
			if (cnt <= 0) {
				order_string += " ";
			} else {
				order_string += ", ";
			}
		}
		plan_string = " ORDER BY " + order_string + plan_string;
		for (auto child : node->children) {
			generateString_t(child, plan_string, additional_cols, true);
		}
		break;
	}
	case DuckASTExpressionType::AGGREGATE: {
		auto exp = dynamic_cast<DuckASTAggregate *>(node->expr.get());
		if (has_filter)
			plan_string = " having " + plan_string;
		string grp_string = "";
		if (exp->is_group_by) {
			int count = exp->group_column.size();
			for (auto grp : exp->group_column) {
				grp_string += grp;
				count--;
				if (count <= 0) {
					grp_string += " ";
				} else {
					grp_string += ", ";
				}
			}
			plan_string = " group by " + grp_string + plan_string;
		}
		for (auto ext_col : exp->aggregate_function) {
			additional_cols.push_back(ext_col);
		}
		for (auto child : node->children) {
			generateString_t(child, plan_string, additional_cols);
		}
		break;
	}
	case DuckASTExpressionType::GET: {
		vector<string> columns;
		auto exp = dynamic_cast<DuckASTGet *>(node->expr.get());
		string table_name = exp->table_name;
		if (has_filter) {
			plan_string = " where " + plan_string;
		}
		if (exp->all_columns) {
			plan_string = "select * from " + table_name + " " + plan_string;
			return;
		}
		for (auto col : exp->alias_map) {
			if (col.second == "") {
				columns.push_back(col.first);
			} else {
				columns.push_back(col.first + " as " + col.second);
			}
		}
		string cur_string = "select ";
		for (int i = 0; i < columns.size(); i++) {
			cur_string += columns[i];
			if (i != columns.size() - 1) {
				cur_string += ", ";
			} else if (additional_cols.size() != 0) {
				cur_string += ", ";
			}
		}
		for (int i = 0; i < additional_cols.size(); i++) {
			cur_string += additional_cols[i];
			if (i != additional_cols.size() - 1) {
				cur_string += ", ";
			}
		}
		cur_string += " from " + table_name;
		plan_string = cur_string + plan_string;

		// Assuming that this is the bottom of the tree
		return;
	}
	}
}

void DuckAST::generateString(string &plan_string) {
	if (root == nullptr)
		return;
	vector<string> addcols;
	this->generateString_t(root, plan_string, addcols);
	plan_string += ";";
}


void DuckAST::printAST(shared_ptr<duckdb::DuckASTNode> node, string prefix, bool isLast) {
	std::cout << prefix;
	std::cout << (isLast ? "└── " : "├── ");
	if (node->expr != nullptr) {
		std::cout << "Node: " << node->Name() << ", Type: " << node->type << ", Expression: " << node->expr->name << std::endl;
	} else {
		std::cout << "Node ID: " << node->id << ", Type: " << node->type << std::endl;
	}

	// Recursively print children
	for (size_t i = 0; i < node->children.size(); ++i) {
		printAST(node->children[i], prefix + (isLast ? "    " : "│   "), i == node->children.size() - 1);
	}
}

void DuckAST::printAST(shared_ptr<duckdb::DuckAST> ast) {
	// Print AST starting from the root
	if (ast->root != nullptr) {
		std::cout << "root\n" << std::endl;
		printAST(ast->root, "", false);
	} else {
		std::cout << "Empty AST" << std::endl;
	}
}

// DuckASTNode DuckAST::get_node(string node_id, shared_ptr<DuckASTNode> node=nullptr) {
//    }

} // namespace duckdb
