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
		this->column_names;
    }

	DuckASTProjection::~DuckASTProjection() {
	}

	void DuckASTProjection::add_column(string table_index, string column_index, string alias) {
		column_alaises[table_index + "." + column_index] = alias;
	}

	// DuckASTGet
	DuckASTGet::DuckASTGet() {
		this->all_columns = false;
	}

	DuckASTGet::DuckASTGet(string table_name, unsigned long int table_index, std::vector<string> col_name) {
		this->table_name = table_name;
		this->table_index = table_index;
		this->column_names = column_names;
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
			bool result = insert_after_root(root, parent_id, child);
			if(result) return true;
		}
		return false;
    }


	void DuckAST::insert(shared_ptr<DuckASTBaseExpression>& expr, string id, DuckASTExpressionType type, string parent_id) {
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
		}
    }

	void DuckAST::displayTree_t(shared_ptr<DuckASTNode> node) {
	    if(node == nullptr) return;
		Printer::Print(node->id);
		Printer::Print("\t->");

		// Cast according to Nde Type
		switch(node->type) {
			case DuckASTExpressionType::GET: {
				auto exp = dynamic_cast<DuckASTGet *>(node->expr.get());
				Printer::Print(exp->table_name);
				Printer::Print("Columns: ");
				for(auto col: exp->column_names) {
					Printer::Print(col);
				}
			}
			case DuckASTExpressionType::PROJECTION: {
				auto exp = dynamic_cast<DuckASTProjection*>(node->expr.get());
				Printer::Print("Columns to select:");
				for(auto col: exp->column_names) {
					Printer::Print(col);
				}
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

}