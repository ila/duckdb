#include "data_representation.hpp"

#include <iostream>

/*
 * Refer: https://gist.github.com/destrex271/b3693b54bce6035749228aa45709b0e1 for extra notes
 */

namespace duckdb {
// DuckASTBaseOperator
DuckASTBaseOperator::DuckASTBaseOperator() {
}

DuckASTBaseOperator::DuckASTBaseOperator(string name) {
	this->name = name;
}

DuckASTBaseOperator::~DuckASTBaseOperator() {
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

// DuckASTJoin functions
void DuckASTJoin::add_table(string table_name) {
	// Reference from table index to table name
	this->tables.push_back(table_name);
}

void DuckASTJoin::set_condition(string &condition) {
	this->condition = condition;
}

// DuckASTGet
DuckASTGet::DuckASTGet() {
	this->all_columns = false;
}

DuckASTGet::DuckASTGet(string table_name) {
	this->table_name = table_name;
	this->all_columns = false;
	this->filter_condition = "";
}

void DuckASTGet::set_table_name(string table_name) {
	this->table_name = table_name;
}

DuckASTGet::~DuckASTGet() {
}

DuckASTInsert::DuckASTInsert() {
}

DuckASTInsert::~DuckASTInsert() {
}

void DuckASTInsert::set_table_name(string t) {
	this->table_name = t;
}
DuckASTInsert::DuckASTInsert(string t) {
	this->table_name = t;
}

// DuckASTNode
DuckASTNode::DuckASTNode() {
	this->opr = nullptr;
	this->type = DuckASTOperatorType::NONE;
}

DuckASTNode::DuckASTNode(shared_ptr<DuckASTBaseOperator> opr, DuckASTOperatorType type) {
	this->opr = opr;
	this->type = type;
	this->name = opr->name;
}

void DuckASTNode::setExpression(shared_ptr<DuckASTBaseOperator> opr, DuckASTOperatorType type) {
	this->opr = std::move(opr);
	this->type = type;
	this->name = opr->name;
}

// DuckAST
DuckAST::DuckAST() {
	root = nullptr;
}

// Uses the parent node pointer provided and appends to its list of children
void DuckAST::insert(shared_ptr<DuckASTBaseOperator> &opr, shared_ptr<DuckASTNode> &parent_node, string id,
                     DuckASTOperatorType type) {
	// Printer::Print("Inserting: " + id);
	opr->name = id;
	if (root == nullptr && parent_node == nullptr) {
		root = (shared_ptr<DuckASTNode>)(new DuckASTNode(opr, type));
		root->type = type;
		root->parent_node = nullptr;
		this->last_ptr = root;
		return;
	}

	auto node = (shared_ptr<DuckASTNode>)(new DuckASTNode(opr, type));
	node->parent_node = parent_node;
	parent_node->children.push_back(node);
	this->last_ptr = node;
}

// Returns the last node in the entire AST.
// Helps in appending new nodes
shared_ptr<DuckASTNode> DuckAST::getLastNode() {
	return last_ptr;
}

// Primary function which recursively generates a valid sql string from the AST
void DuckAST::generateString(const shared_ptr<DuckASTNode> &node, string &prefix_string, string &plan_string,
                             bool has_filter, int join_child_index) {
	if (node == nullptr) {
		return;
	}

	// insert into my_table values(1), (2), (3);
	// select my_column from my_table where ...
	// insert into other_table select my_column from my_table where ...

	// create table as ...
	// with my_table as (select my_column from my_table where ...)
	// delete from my_table where ...

	// Append to plan_string according to node type
	switch (node->type) {
	case DuckASTOperatorType::CROSS_JOIN: {
		auto exp = dynamic_cast<DuckASTJoin *>(node->opr.get());
		string tables = "";
		int cnt = 0;
		for (auto &table : exp->tables) {
			tables += " " + table + ", ";
			cnt++;
		}
		if (has_filter) {
			plan_string = "where " + plan_string;
		}
		plan_string = " from " + tables.substr(0, tables.size() - 2) + " " + plan_string;

		for (int i = node->children.size() - 1; i >= 0; i--) {
			auto &child = node->children[i];
			generateString(child, prefix_string, plan_string, false, i);
		}
		break;
	}
	case DuckASTOperatorType::PROJECTION: {
		for (const auto &child : node->children) {
			generateString(child, prefix_string, plan_string);
		}
		break;
	}
	case DuckASTOperatorType::FILTER: {
		auto exp = dynamic_cast<DuckASTFilter *>(node->opr.get());
		plan_string = exp->filter_condition + plan_string;
		auto children = node->children;
		for (const auto &child : node->children) {
			generateString(child, prefix_string, plan_string, true);
		}
		break;
	}
	case DuckASTOperatorType::ORDER_BY: {
		auto exp = dynamic_cast<DuckASTOrderBy *>(node->opr.get());
		string order_string = "";
		int cnt = exp->order.size();
		for (const auto &ord : exp->order) {
			order_string += ord.first + " " + ord.second;
			cnt--;
			if (cnt <= 0) {
				order_string += " ";
			} else {
				order_string += ", ";
			}
		}
		plan_string = " order by " + order_string + plan_string;
		for (const auto &child : node->children) {
			generateString(child, prefix_string, plan_string, true);
		}
		break;
	}
	case DuckASTOperatorType::AGGREGATE: {
		auto exp = dynamic_cast<DuckASTAggregate *>(node->opr.get());
		if (has_filter) {
			plan_string = " having " + plan_string;
		}
		string grp_string = "";
		if (exp->is_group_by) {
			int count = exp->group_column.size();
			for (const auto &grp : exp->group_column) {
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
		for (const auto &child : node->children) {
			generateString(child, prefix_string, plan_string);
		}
		break;
	}
	case DuckASTOperatorType::GET: {
		vector<string> columns;
		auto exp = dynamic_cast<DuckASTGet *>(node->opr.get());
		string table_name = exp->table_name;
		if (!exp->filter_condition.empty() && has_filter) {
			plan_string = " where " + plan_string + " and " + exp->filter_condition;
		} else if (has_filter) {
			plan_string = " where " + plan_string;
		} else if (!exp->filter_condition.empty()) {
			plan_string = " where " + exp->filter_condition;
		}
		if (exp->all_columns) {
			plan_string = "select * from " + table_name + " " + plan_string;
			return;
		}

		for (auto &pair : exp->column_aliases) {
			if (pair.first == pair.second || pair.second == "duckdb_placeholder_internal") {
				// select_string = select_string + pair.first + ", ";
				columns.push_back(pair.first);
			} else {
				// select_string = select_string + pair.first + " as " + pair.second + ", ";
				// columns.push_back(pair.first + " as " + pair.second);
				columns.push_back(pair.second + " as " + pair.first);
			}
		}

		string cur_string = " ";
		if (join_child_index != 1) {
			cur_string = "select ";
		}
		for (size_t i = 0; i < columns.size(); i++) {
			cur_string += columns[i];
			if (i < columns.size() - 1) {
				cur_string += ", ";
			}
		}

		if (join_child_index == -1) {
			cur_string += " from " + table_name;
			plan_string = prefix_string + cur_string + plan_string;
		} else if (join_child_index == 1) {
			plan_string = prefix_string + cur_string + plan_string;
		} else {
			plan_string = prefix_string + cur_string + ", " + plan_string;
		}

		// Assuming that this is the bottom of the tree
		return;
	}
	case DuckASTOperatorType::INSERT: {
		auto exp = dynamic_cast<DuckASTInsert *>(node->opr.get());
		prefix_string = "insert into " + exp->table_name + " ";
		for (const auto& child : node->children) {
			generateString(child, prefix_string, plan_string);
		}
		break;
	}
	}
}

void DuckAST::generateString(string &plan_string) {
	if (root == nullptr)
		return;
	string prefix_string;
	this->generateString(root, prefix_string, plan_string);
	plan_string += ";";
}

void DuckAST::printAST(shared_ptr<duckdb::DuckASTNode> node, string prefix, bool isLast) {
	std::cout << prefix;
	std::cout << (isLast ? "└── " : "├── ");
	if (node->opr != nullptr) {
		std::cout << "Node: " << node->name << ", Type: " << node->type << ", Operator: " << node->opr->name
		          << std::endl;
	} else {
		std::cout << "Node ID: " << node->name << ", Type: " << node->type << std::endl;
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

} // namespace duckdb
