/* Copyright (c) 2023 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2023/08/16.
//

#include "sql/optimizer/logical_plan_generator.h"

#include <common/log/log.h>

#include <memory>

#include "sql/operator/calc_logical_operator.h"
#include "sql/operator/delete_logical_operator.h"
#include "sql/operator/explain_logical_operator.h"
#include "sql/operator/insert_logical_operator.h"
#include "sql/operator/join_logical_operator.h"
#include "sql/operator/logical_operator.h"
#include "sql/operator/predicate_logical_operator.h"
#include "sql/operator/project_logical_operator.h"
#include "sql/operator/table_get_logical_operator.h"
#include "sql/operator/group_by_logical_operator.h"
#include "sql/operator/update_logical_opeator.h"

#include "sql/stmt/calc_stmt.h"
#include "sql/stmt/delete_stmt.h"
#include "sql/stmt/explain_stmt.h"
#include "sql/stmt/filter_stmt.h"
#include "sql/stmt/insert_stmt.h"
#include "sql/stmt/select_stmt.h"
#include "sql/stmt/update_stmt.h"
#include "sql/stmt/stmt.h"

#include "sql/expr/expression_iterator.h"

using namespace std;
using namespace common;

RC LogicalPlanGenerator::create(Stmt *stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  RC rc = RC::SUCCESS;
  switch (stmt->type()) {
    case StmtType::CALC: {
      auto *calc_stmt = static_cast<CalcStmt *>(stmt);

      rc = create_plan(calc_stmt, logical_operator);
    } break;

    case StmtType::SELECT: {
      auto *select_stmt = static_cast<SelectStmt *>(stmt);

      rc = create_plan(select_stmt, logical_operator);
    } break;

    case StmtType::INSERT: {
      auto *insert_stmt = static_cast<InsertStmt *>(stmt);

      rc = create_plan(insert_stmt, logical_operator);
    } break;

    case StmtType::DELETE: {
      auto *delete_stmt = static_cast<DeleteStmt *>(stmt);

      rc = create_plan(delete_stmt, logical_operator);
    } break;

    case StmtType::UPDATE: {
      auto update_stmt = static_cast<UpdateStmt *>(stmt);

      rc = create_plan(update_stmt, logical_operator);
    } break;

    case StmtType::EXPLAIN: {
      auto *explain_stmt = static_cast<ExplainStmt *>(stmt);

      rc = create_plan(explain_stmt, logical_operator);
    } break;
    default: {
      rc = RC::UNIMPLEMENTED;
    }
  }
  return rc;
}

RC LogicalPlanGenerator::create_plan(CalcStmt *calc_stmt, std::unique_ptr<LogicalOperator> &logical_operator)
{
  logical_operator = std::make_unique<CalcLogicalOperator>(std::move(calc_stmt->expressions()));
  return RC::SUCCESS;
}

RC LogicalPlanGenerator::create_plan(SelectStmt *select_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  unique_ptr<LogicalOperator> *last_oper = nullptr;

  unique_ptr<LogicalOperator> table_oper(nullptr);
  last_oper = &table_oper;

  const std::vector<Table *> &tables = select_stmt->tables();
  for (Table *table : tables) {

    unique_ptr<LogicalOperator> table_get_oper(new TableGetLogicalOperator(table, ReadWriteMode::READ_ONLY));
    if (table_oper == nullptr) {
      table_oper = std::move(table_get_oper);
    } else {
      auto *join_oper = new JoinLogicalOperator;
      join_oper->add_child(std::move(table_oper));
      join_oper->add_child(std::move(table_get_oper));
      table_oper = unique_ptr<LogicalOperator>(join_oper);
    }
  }

  unique_ptr<LogicalOperator> predicate_oper;

  RC rc = create_plan(select_stmt->filter_stmt(), predicate_oper);
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to create predicate logical plan. rc=%s", strrc(rc));
    return rc;
  }

  if (predicate_oper) {
    if (*last_oper) {
      predicate_oper->add_child(std::move(*last_oper)); // predicate -> tableget/join
    }

    last_oper = &predicate_oper;
  }

  unique_ptr<LogicalOperator> group_by_oper;
  rc = create_group_by_plan(select_stmt, group_by_oper);
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to create group by logical plan. rc=%s", strrc(rc));
    return rc;
  }

  if (group_by_oper) {
    if (*last_oper) {
      group_by_oper->add_child(std::move(*last_oper));
    }

    last_oper = &group_by_oper;
  }

  auto project_oper = make_unique<ProjectLogicalOperator>(std::move(select_stmt->query_expressions()));
  if (*last_oper) {
    project_oper->add_child(std::move(*last_oper));
  }

  logical_operator = std::move(project_oper);
  return RC::SUCCESS;
}

RC LogicalPlanGenerator::create_plan(FilterStmt *filter_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  RC                                  rc = RC::SUCCESS;
  std::vector<unique_ptr<Expression>> cmp_exprs;
  auto                               &conditions = filter_stmt->conditions_;
  for (auto &condition : conditions) {

    unique_ptr<Expression> cmp_expr(nullptr);

    switch (condition->type()) {
      case ExprType::COMPARISON: {
        // 先暂时把原本的优化去掉

        // 将子查询的 expr 拿出来创建逻辑算子，并把创建好的算子放回 expr 中
        auto cmp_expr_ = static_cast<ComparisonExpr *>(condition.get());
        // exists / not exists 可能会使得 left_expr 为空
        if (cmp_expr_->left() != nullptr && cmp_expr_->left()->type() == ExprType::SUB_QUERY) {
          auto sub_query_expr = static_cast<SubqueryExpr *>(cmp_expr_->left().get());
          auto sub_query_stmt = static_cast<SelectStmt *>(sub_query_expr->stmt().get());
          unique_ptr<LogicalOperator> sub_query_oper;
          rc = create_plan(sub_query_stmt, sub_query_oper);
          if (rc != RC::SUCCESS) {
            LOG_WARN("failed to create subquery logical operator. rc=%s", strrc(rc));
            return rc;
          }
          sub_query_expr->set_logical_operator(std::move(sub_query_oper));
        } else if (cmp_expr_->right() != nullptr && cmp_expr_->right()->type() == ExprType::SUB_QUERY) {
          auto sub_query_expr = static_cast<SubqueryExpr *>(cmp_expr_->right().get());
          auto sub_query_stmt = static_cast<SelectStmt *>(sub_query_expr->stmt().get());
          unique_ptr<LogicalOperator> sub_query_oper;
          rc = create_plan(sub_query_stmt, sub_query_oper);
          if (rc != RC::SUCCESS) {
            LOG_WARN("failed to create subquery logical operator. rc=%s", strrc(rc));
            return rc;
          }
          sub_query_expr->set_logical_operator(std::move(sub_query_oper));
        }
        
        cmp_expr = unique_ptr<ComparisonExpr>(static_cast<ComparisonExpr *>(condition.release()));
      } break;
      case ExprType::LIKE: {
        cmp_expr = unique_ptr<LikeExpr>(static_cast<LikeExpr *>(condition.release()));
      } break;
      case ExprType::IS: {
        cmp_expr = unique_ptr<IsExpr>(static_cast<IsExpr *>(condition.release()));
      } break;
      default: {
        LOG_ERROR("invalid condition type, type=%s", expr_type_to_string(condition->type()).c_str());
        return RC::INVALID_ARGUMENT;
      }
    }
    cmp_exprs.emplace_back(std::move(cmp_expr));

    // 以下是原本的优化逻辑，先暂时去掉
    // if (left_value_type != right_value_type) {
    //   auto left_to_right_cost = implicit_cast_cost(left_value_type, right_value_type);
    //   auto right_to_left_cost = implicit_cast_cost(right_value_type, left_value_type);
    //   if (left_to_right_cost <= right_to_left_cost && left_to_right_cost != INT32_MAX) {
    //     auto cast_expr = make_unique<CastExpr>(std::move(left_expr), right_value_type);
    //     if (left_expr->type() == ExprType::VALUE) {
    //       Value left_val;
    //       if (OB_FAIL(rc = cast_expr->try_get_value(left_val))) {
    //         LOG_WARN("failed to get value from left child", strrc(rc));
    //         return rc;
    //       }
    //       left_expr = make_unique<ValueExpr>(left_val);
    //     } else {
    //       left_expr = std::move(cast_expr);
    //     }
    //   } else if (right_to_left_cost < left_to_right_cost && right_to_left_cost != INT32_MAX) {
    //     right_expr = make_unique<CastExpr>(std::move(right_expr), left_value_type);
    //   } else {
    //     rc = RC::UNSUPPORTED;
    //     LOG_WARN("unsupported cast from %s to %s", attr_type_to_string(left_value_type),
    //     attr_type_to_string(right_value_type)); return rc;
    //   }
    // }
  }

  unique_ptr<PredicateLogicalOperator> predicate_oper;
  if (!cmp_exprs.empty()) {
    unique_ptr<ConjunctionExpr> conjunction_expr(new ConjunctionExpr(ConjunctionExpr::Type::AND, cmp_exprs));
    predicate_oper = std::make_unique<PredicateLogicalOperator>(std::move(conjunction_expr));
  }

  logical_operator = std::move(predicate_oper);
  return rc;
}

// 计算转换的代价
int LogicalPlanGenerator::implicit_cast_cost(AttrType from, AttrType to)
{
  if (from == to) {
    return 0;
  }
  return DataType::type_instance(from)->cast_cost(to);
}

RC LogicalPlanGenerator::create_plan(InsertStmt *insert_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  Table        *table = insert_stmt->table();
  vector<Value> values(insert_stmt->values(), insert_stmt->values() + insert_stmt->value_amount());

  auto *insert_operator = new InsertLogicalOperator(table, values);
  logical_operator.reset(insert_operator);
  return RC::SUCCESS;
}

RC LogicalPlanGenerator::create_plan(DeleteStmt *delete_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  Table                      *table       = delete_stmt->table();
  FilterStmt                 *filter_stmt = delete_stmt->filter_stmt();
  unique_ptr<LogicalOperator> table_get_oper(new TableGetLogicalOperator(table, ReadWriteMode::READ_WRITE));

  unique_ptr<LogicalOperator> predicate_oper;

  RC rc = create_plan(filter_stmt, predicate_oper);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  unique_ptr<LogicalOperator> delete_oper(new DeleteLogicalOperator(table));

  if (predicate_oper) {
    predicate_oper->add_child(std::move(table_get_oper));
    delete_oper->add_child(std::move(predicate_oper));
  } else {
    delete_oper->add_child(std::move(table_get_oper));
  }

  logical_operator = std::move(delete_oper);
  return rc;
}

RC LogicalPlanGenerator::create_plan(ExplainStmt *explain_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  unique_ptr<LogicalOperator> child_oper;

  Stmt *child_stmt = explain_stmt->child();

  RC rc = create(child_stmt, child_oper);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create explain's child operator. rc=%s", strrc(rc));
    return rc;
  }

  logical_operator = unique_ptr<LogicalOperator>(new ExplainLogicalOperator);
  logical_operator->add_child(std::move(child_oper));
  return rc;
}

RC LogicalPlanGenerator::create_group_by_plan(SelectStmt *select_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  vector<unique_ptr<Expression>> &group_by_expressions = select_stmt->group_by();
  vector<Expression *> aggregate_expressions;
  vector<unique_ptr<Expression>> &query_expressions = select_stmt->query_expressions();
  function<RC(std::unique_ptr<Expression>&)> collector = [&](unique_ptr<Expression> &expr) -> RC {
    RC rc = RC::SUCCESS;
    if (expr->type() == ExprType::AGGREGATION) {
      expr->set_pos(aggregate_expressions.size() + group_by_expressions.size());
      aggregate_expressions.push_back(expr.get());
    }
    rc = ExpressionIterator::iterate_child_expr(*expr, collector);
    return rc;
  };

  function<RC(std::unique_ptr<Expression>&)> bind_group_by_expr = [&](unique_ptr<Expression> &expr) -> RC {
    RC rc = RC::SUCCESS;
    for (size_t i = 0; i < group_by_expressions.size(); i++) {
      auto &group_by = group_by_expressions[i];
      if (expr->type() == ExprType::AGGREGATION) {
        break;
      } else if (expr->equal(*group_by)) {
        expr->set_pos(i);
        continue;
      } else {
        rc = ExpressionIterator::iterate_child_expr(*expr, bind_group_by_expr);
      }
    }
    return rc;
  };

  bool                                        found_unbound_column = false;
  function<RC(std::unique_ptr<Expression> &)> find_unbound_column  = [&](unique_ptr<Expression> &expr) -> RC {
    RC rc = RC::SUCCESS;
    if (expr->type() == ExprType::AGGREGATION) {
      // do nothing
    } else if (expr->pos() != -1) {
      // do nothing
    } else if (expr->type() == ExprType::UNBOUND_FIELD || expr->type() == ExprType::UNBOUND_AGGREGATION) {
      found_unbound_column = true;
    } else {
      rc = ExpressionIterator::iterate_child_expr(*expr, find_unbound_column);
    }
    return rc;
  };

  for (unique_ptr<Expression> &expression : query_expressions) {
    bind_group_by_expr(expression);
  }

  for (unique_ptr<Expression> &expression : query_expressions) {
    find_unbound_column(expression);
  }

  // collect all aggregate expressions
  for (unique_ptr<Expression> &expression : query_expressions) {
    collector(expression);
  }

  if (group_by_expressions.empty() && aggregate_expressions.empty()) {
    // 既没有group by也没有聚合函数，不需要group by
    return RC::SUCCESS;
  }

  if (found_unbound_column) {
    LOG_WARN("column must appear in the GROUP BY clause or must be part of an aggregate function");
    return RC::INVALID_ARGUMENT;
  }

  // 如果只需要聚合，但是没有group by 语句，需要生成一个空的group by 语句

  auto group_by_oper = make_unique<GroupByLogicalOperator>(std::move(group_by_expressions),
                                                           std::move(aggregate_expressions));
  logical_operator = std::move(group_by_oper);
  return RC::SUCCESS;
}

RC LogicalPlanGenerator::create_plan(UpdateStmt *update_stmt, std::unique_ptr<LogicalOperator> &logical_operator)
{
  Table                      *table = update_stmt->table();
  unique_ptr<LogicalOperator> table_get_operator(new TableGetLogicalOperator(table, ReadWriteMode::READ_WRITE));

  unique_ptr<LogicalOperator> predicate_operator;
  RC                          rc = create_plan(update_stmt->filter_stmt(), predicate_operator);
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to create predicate logical plan. rc=%s", strrc(rc));
    return rc;
  }
  logical_operator = std::make_unique<UpdateLogicalOperator>(
      update_stmt->table(), update_stmt->field_metas(), std::move(update_stmt->exprs()));
  // update 算子 -> select 算子 -> 扫表
  if (predicate_operator) {
    predicate_operator->add_child(std::move(table_get_operator));
    logical_operator->add_child(std::move(predicate_operator));
  } else {
    logical_operator->add_child(std::move(table_get_operator));
  }
  return RC::SUCCESS;
}