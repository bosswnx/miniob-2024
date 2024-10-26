/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2022/5/22.
//

#include "sql/stmt/filter_stmt.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "common/rc.h"
#include "sql/parser/expression_binder.h"
#include "storage/db/db.h"
#include "storage/table/table.h"

FilterStmt::~FilterStmt() { conditions_.clear(); }

RC FilterStmt::create(Db *db, Table *default_table, std::unordered_map<std::string, Table *> *tables,
    std::vector<ConditionSqlNode> &conditions, FilterStmt *&stmt)
{
  RC rc = RC::SUCCESS;
  stmt  = nullptr;

  // 从 ConditionSqlNode 创建 ComparisonExpr 和 LikeExpr
  vector<unique_ptr<Expression>> conditions_exprs;
  for (auto &condition : conditions) {
    switch (condition.comp_op) {
      case CompOp::LIKE: {
        conditions_exprs.emplace_back(
            new LikeExpr(true, std::move(condition.left_expr), std::move(condition.right_expr)));
      } break;
      case CompOp::NOT_LIKE: {
        conditions_exprs.emplace_back(
            new LikeExpr(false, std::move(condition.left_expr), std::move(condition.right_expr)));
      } break;
      case CompOp::EQUAL_TO:
      case CompOp::LESS_EQUAL:
      case CompOp::NOT_EQUAL:
      case CompOp::LESS_THAN:
      case CompOp::GREAT_EQUAL:
      case CompOp::GREAT_THAN: {
        conditions_exprs.emplace_back(
            new ComparisonExpr(condition.comp_op, std::move(condition.left_expr), std::move(condition.right_expr)));
      } break;
      default: {
        LOG_WARN("unsupported condition operator. comp_op=%d", condition.comp_op);
        return RC::INVALID_ARGUMENT;
      }
    }
  }

  // 绑定表达式
  BinderContext binder_context;
  for (auto &table : *tables) {
    binder_context.add_table(table.second);
  }
  ExpressionBinder expression_binder(binder_context);

  vector<unique_ptr<Expression>> bound_conditions;

  auto *tmp_stmt = new FilterStmt();
  for (size_t i = 0; i < conditions.size(); i++) {
    RC rc = expression_binder.bind_expression(conditions_exprs[i], bound_conditions);
    if (rc != RC::SUCCESS) {
      delete tmp_stmt;
      LOG_WARN("failed to create filter unit. condition index=%d", i);
      return rc;
    }
  }

  tmp_stmt->conditions_.swap(bound_conditions);

  stmt = tmp_stmt;
  return rc;
}

RC get_table_and_field(Db *db, Table *default_table, std::unordered_map<std::string, Table *> *tables,
    const RelAttrSqlNode &attr, Table *&table, const FieldMeta *&field)
{
  if (common::is_blank(attr.relation_name.c_str())) {
    table = default_table;
  } else if (nullptr != tables) {
    auto iter = tables->find(attr.relation_name);
    if (iter != tables->end()) {
      table = iter->second;
    }
  } else {
    table = db->find_table(attr.relation_name.c_str());
  }
  if (nullptr == table) {
    LOG_WARN("No such table: attr.relation_name: %s", attr.relation_name.c_str());
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  field = table->table_meta().field(attr.attribute_name.c_str());
  if (nullptr == field) {
    LOG_WARN("no such field in table: table %s, field %s", table->name(), attr.attribute_name.c_str());
    table = nullptr;
    return RC::SCHEMA_FIELD_NOT_EXIST;
  }

  return RC::SUCCESS;
}
