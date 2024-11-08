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
// Created by Wangyunlai on 2022/6/5.
//

#pragma once

#include <memory>
#include <vector>

#include "common/rc.h"
#include "sql/expr/expression.h"
#include "sql/stmt/stmt.h"
#include "storage/field/field.h"

class FieldMeta;
class FilterStmt;
class Db;
class Table;

/**
 * @brief 表示select语句
 * @ingroup Statement
 */
class SelectStmt : public Stmt
{
public:
  SelectStmt() = default;
  ~SelectStmt() override;

  StmtType type() const override { return StmtType::SELECT; }

public:
  static RC create(Db *db, SelectSqlNode &select_sql, Stmt *&stmt,
  std::shared_ptr<std::unordered_map<string, string>> name2alias = nullptr,
  std::shared_ptr<std::unordered_map<string, string>> alias2name = nullptr,
  std::shared_ptr<std::vector<string>> loaded_relation_names = nullptr,
  std::shared_ptr<std::unordered_map<string, string>> field_alias2name = nullptr);
  static RC convert_alias_to_name(Expression *expr, 
    std::shared_ptr<std::unordered_map<string, string>> alias2name,
    std::shared_ptr<std::unordered_map<string, string>> field_alias2name);

public:
  const std::vector<Table *> &tables() const { return tables_; }
  FilterStmt                 *filter_stmt() const { return filter_stmt_; }
  FilterStmt                 *filter_stmt_having() const { return filter_stmt_having_; }

  std::vector<std::unique_ptr<Expression>> &query_expressions() { return query_expressions_; }
  std::vector<std::unique_ptr<Expression>> &group_by() { return group_by_; }
  std::vector<std::unique_ptr<Expression>> &order_by_exprs() { return order_by_exprs_; }
  std::vector<bool>                        &order_by_descs() { return order_by_descs_; }

  std::vector<std::unique_ptr<Expression>> query_expressions_;
  std::vector<Table *>                     tables_;
  std::vector<std::string>                 table_alias_;
  FilterStmt                              *filter_stmt_ = nullptr;
  std::vector<std::unique_ptr<Expression>> group_by_;
  FilterStmt                              *filter_stmt_having_ = nullptr;
  std::vector<std::unique_ptr<Expression>> order_by_exprs_;
  std::vector<bool>                        order_by_descs_;
  int                                      limit_ = -1;  // -1 表示不限制

  std::vector<FieldMeta> get_query_fields() {
    // 此方法用于 Create-Table-Select
    std::vector<FieldMeta> query_fields;

    for (auto &expr : query_expressions_) {
      if (expr->type() == ExprType::FIELD) {
        // 此时已经将所有的 UnboundFieldExpr 转换为 FieldExpr
        auto field_expr = static_cast<FieldExpr *>(expr.get());
        FieldMeta field_meta(*field_expr->field().meta());
        // field_meta.table_name_ = *field_expr.table_name(); // 犯错误了
        field_meta.table_name_ = (*field_expr).table_name(); // 记录表名
        if (!expr->alias_std_string().empty()) {
          // 别名覆盖字段名
          field_meta.set_name(expr->alias_std_string().c_str());
        }
        query_fields.push_back(field_meta);
      } else {
        FieldMeta field_meta;
        std::string field_name;
        if (!expr->alias_std_string().empty()) {
          // 别名覆盖字段名
          field_name = expr->alias_std_string();
        } else {
          field_name = expr->name();
        }
        field_meta.init(field_name.c_str(), expr->value_type(), 0, expr->value_length(), true, 0);
        query_fields.push_back(field_meta);
      }
    }

    return query_fields;
  }

  bool has_special_queries() {
    LOG_DEBUG("expr size: %d", query_expressions_.size());
    for (auto &expr : query_expressions_) {
      LOG_DEBUG("expr type: %d", expr->type());
      if (expr->type() == ExprType::AGGREGATION ||
          expr->type() == ExprType::ARITHMETIC) {
        return true;
      }
    }
    return false;
  }

  bool has_join() {
    return tables_.size() > 1;
  }
};
