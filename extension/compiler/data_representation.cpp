#include "data_representation.hpp"

namespace duckdb {
	// QuackQLBaseExpression
	QuackQLBaseExpression::QuackQLBaseExpression() {
		Printer::Print("Base Expression");
	}

	QuackQLBaseExpression::QuackQLBaseExpression(string name) {
	    this->name = name;
    }

	QuackQLBaseExpression::~QuackQLBaseExpression() {
	}

	// QuackQLProjection
	QuackQLProjection::QuackQLProjection() {
	}

	QuackQLProjection::QuackQLProjection(string name) {
		this->name = name;
    }

	QuackQLProjection::~QuackQLProjection() {
	}

	// QuackQLNode
	QuackQLNode::QuackQLNode() {
		this->expr = nullptr;
		this->type = QuackQLExpressionType::NONE;
    }

	QuackQLNode::QuackQLNode(shared_ptr<QuackQLBaseExpression> expr, QuackQLExpressionType type) {
		this->expr = std::move(expr);
		this->type = type;
    }

	void QuackQLNode::setExpression(shared_ptr<QuackQLBaseExpression> expr, QuackQLExpressionType type) {
		this->expr = std::move(expr);
		this->type = type;
    }

	// QuackQLTree
	QuackQLTree::QuackQLTree() {
		root = nullptr;
    }

	void QuackQLTree::insert(shared_ptr<QuackQLBaseExpression>& expr, string id, QuackQLExpressionType type) {
		expr->name = id;
		if(root == nullptr) {
			root = (shared_ptr<QuackQLNode>)(new QuackQLNode(expr, type));
			root->type = type;
			return;
		}
		Printer::Print("At root right now");
		// todo: Add more insert and stuff logic
    }



}