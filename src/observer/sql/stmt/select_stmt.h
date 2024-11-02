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
  FilterStmt                              *filter_stmt_ = nullptr;
  std::vector<std::unique_ptr<Expression>> group_by_;
  FilterStmt                              *filter_stmt_having_ = nullptr;
  std::vector<std::unique_ptr<Expression>> order_by_exprs_;
  std::vector<bool>                        order_by_descs_;
};
