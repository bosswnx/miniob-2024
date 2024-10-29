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
// Created by Wangyunlai on 2022/6/6.
//

#include "sql/stmt/select_stmt.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "sql/expr/expression.h"
#include "sql/stmt/filter_stmt.h"
#include "storage/db/db.h"
#include "storage/table/table.h"
#include "sql/parser/expression_binder.h"
#include <memory>

using namespace std;
using namespace common;

SelectStmt::~SelectStmt()
{
  if (nullptr != filter_stmt_) {
    delete filter_stmt_;
    filter_stmt_ = nullptr;
  }
}

static RC check_sub_select_legal(Db *db, ParsedSqlNode *sub_select, std::vector<RelationSqlNode> main_query_relations)
{
  // 这个方法主要是检查子查询的合法性：子查询的查询的属性只能有一个。
  FieldExpr *field_expr = nullptr;
  StarExpr  *star_expr   = nullptr;
  for (auto &expr : sub_select->selection.expressions) {
    if (field_expr != nullptr) {
      // 当左子查询的属性不止一个时，报错
      LOG_WARN("invalid subquery attributes. It should be only one");
      return RC::INVALID_ARGUMENT;
    }
    if (expr->type() == ExprType::FIELD) {
      field_expr = static_cast<FieldExpr *>(expr.get());
    } else if (expr->type() == ExprType::STAR) {
      star_expr = static_cast<StarExpr *>(expr.get());
    }
  }
  if (field_expr != nullptr && star_expr != nullptr) {
    LOG_WARN("star_expr and field_expr cannot be used together in subquery");
    return RC::INVALID_ARGUMENT;
  }

  if (star_expr != nullptr) {
    // 如果是 *，需要先获得 table，然后看其中的 fields 的大小是不是 1，如果不是，报错
    int fields_num = 0;
    for (size_t j = 0; j < sub_select->selection.relations.size(); ++j) {
      const char *table_name = sub_select->selection.relations[j].name.c_str();
      if (nullptr == table_name) {
        LOG_WARN("invalid argument. relation name is null. index=%d", j);
        return RC::INVALID_ARGUMENT;
      }
      Table *table = db->find_table(table_name);
      if (nullptr == table) {
        LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
        return RC::SCHEMA_TABLE_NOT_EXIST;
      }
      fields_num += table->table_meta().field_num();
    }
    if (fields_num != 1) {
      LOG_WARN("invalid subquery attributes");
      return RC::INVALID_ARGUMENT;
    }
  }
  return RC::SUCCESS;
}

RC SelectStmt::create(Db *db, SelectSqlNode &select_sql, Stmt *&stmt)
{
  if (nullptr == db) {
    LOG_WARN("invalid argument. db is null");
    return RC::INVALID_ARGUMENT;
  }

  BinderContext binder_context;

  // collect tables in `from` statement
  vector<Table *>                tables;
  unordered_map<string, Table *> table_map;
  for (size_t i = 0; i < select_sql.relations.size(); i++) {
    const char *table_name = select_sql.relations[i].name.c_str();
    if (nullptr == table_name) {
      LOG_WARN("invalid argument. relation name is null. index=%d", i);
      return RC::INVALID_ARGUMENT;
    }

    Table *table = db->find_table(table_name);
    if (nullptr == table) {
      LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
      return RC::SCHEMA_TABLE_NOT_EXIST;
    }

    binder_context.add_table(table);
    tables.push_back(table);
    table_map.insert({table_name, table});
  }

  // 如果有聚合表达式，检查非聚合表达式是否在 group by 语句中
  // 目前只能判断简单的情况，无法判断嵌套的聚合表达式
  bool has_aggregation = false;
  for (unique_ptr<Expression> &expression : select_sql.expressions) {
    if (expression->type() == ExprType::UNBOUND_AGGREGATION) {
      has_aggregation = true;
      break;
    }
  }
  if (has_aggregation) {
    for (unique_ptr<Expression> &select_expr : select_sql.expressions) {
      if (select_expr->type() == ExprType::UNBOUND_AGGREGATION) {
        continue;
      }
      bool found = false;
      for (unique_ptr<Expression> &group_by_expr : select_sql.group_by) {
        if (select_expr->equal(*group_by_expr)) {
          found = true;
          break;
        }
      }
      if (!found) {
        LOG_WARN("non-aggregation expression found in select statement but not in group by statement");
        return RC::INVALID_ARGUMENT;
      }
    }
  }

  // 接下来是绑定表达式，指的是将表达式中的字段和 table 关联起来

  // collect query fields in `select` statement
  vector<unique_ptr<Expression>> bound_expressions;
  ExpressionBinder               expression_binder(binder_context);

  // 遍历 select 语句中的要查询的字段的表达式, 绑定表达式
  for (unique_ptr<Expression> &expression : select_sql.expressions) {
    RC rc = expression_binder.bind_expression(expression, bound_expressions);
    if (OB_FAIL(rc)) {
      LOG_INFO("bind expression failed. rc=%s", strrc(rc));
      return rc;
    }
  }

  // 遍历 group by 语句中的表达式, 绑定表达式
  vector<unique_ptr<Expression>> group_by_expressions;
  for (unique_ptr<Expression> &expression : select_sql.group_by) {
    RC rc = expression_binder.bind_expression(expression, group_by_expressions);
    if (OB_FAIL(rc)) {
      LOG_INFO("bind expression failed. rc=%s", strrc(rc));
      return rc;
    }
  }

  // 子查询，遍历 conditions 中的表达式，（递归）创建对应的 stmt。
  // 这个 for 会将所有的子查询的 stmt 都创建好，放到 SubqueryExpr 中
  for (auto &condition : select_sql.conditions) {
    // exists/not exists 可能会使得 left_expr 为空
    if (condition.left_expr != nullptr && condition.left_expr->type() == ExprType::SUB_QUERY) {
      SubqueryExpr *subquery_expr = static_cast<SubqueryExpr *>(condition.left_expr.get());
      Stmt         *stmt          = nullptr;
      RC            rc            = SelectStmt::create(db, subquery_expr->sub_query_sn()->selection, stmt);
      if (rc != RC::SUCCESS) {
        LOG_WARN("cannot construct subquery stmt");
        return rc;
      }
      // 检查子查询的合法性：子查询的查询的属性只能有一个
      RC rc_ = check_sub_select_legal(db, subquery_expr->sub_query_sn(), select_sql.relations);
      if (rc_ != RC::SUCCESS) {
        return rc_;
      }
      subquery_expr->set_stmt(unique_ptr<SelectStmt>(static_cast<SelectStmt *>(stmt)));
    } else if (condition.right_expr != nullptr && condition.right_expr->type() == ExprType::SUB_QUERY) {
      SubqueryExpr *subquery_expr = static_cast<SubqueryExpr *>(condition.right_expr.get());
      Stmt         *stmt          = nullptr;
      RC            rc            = SelectStmt::create(db, subquery_expr->sub_query_sn()->selection, stmt);
      if (rc != RC::SUCCESS) {
        LOG_WARN("cannot construct subquery stmt");
        return rc;
      }
      // 检查子查询的合法性：子查询的查询的属性只能有一个
      RC rc_ = check_sub_select_legal(db, subquery_expr->sub_query_sn(), select_sql.relations);
      if (rc_ != RC::SUCCESS) {
        return rc_;
      }
      subquery_expr->set_stmt(unique_ptr<SelectStmt>(static_cast<SelectStmt *>(stmt)));
    }
  }

  Table *default_table = nullptr;
  if (tables.size() == 1) {
    default_table = tables[0];
  }

  // create filter statement in `where` statement
  FilterStmt *filter_stmt = nullptr;
  RC          rc          = FilterStmt::create(db, default_table, &table_map, select_sql.conditions, filter_stmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("cannot construct filter stmt");
    return rc;
  }

  // everything alright
  auto *select_stmt = new SelectStmt();

  select_stmt->tables_.swap(tables);
  select_stmt->query_expressions_.swap(bound_expressions);
  select_stmt->filter_stmt_ = filter_stmt;
  select_stmt->group_by_.swap(group_by_expressions);
  stmt = select_stmt;
  return RC::SUCCESS;
}
