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
// Created by Soulter on 2024/11/5.
//

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "sql/stmt/stmt.h"
#include "sql/operator/physical_operator.h"
#include "sql/parser/parse_defs.h"

class Db;

/**
 * @brief 表示创建表的语句
 * @ingroup Statement
 * @details 虽然解析成了stmt，但是与原始的SQL解析后的数据也差不多
 */
class CreateViewStmt : public Stmt
{
public:
  explicit CreateViewStmt(const std::string &view_name, const std::vector<std::string> &attrs_name)
      : view_name_(view_name), attrs_name_(attrs_name)
  {}
  virtual ~CreateViewStmt() = default;

  StmtType type() const override { return StmtType::CREATE_VIEW; }

  const std::string &view_name() const { return view_name_; }
  const std::vector<std::string> &attrs_name() const { return attrs_name_; }
  const std::string &view_definition() const { return view_definition_; }

  void set_select_stmt(SelectStmt *select_stmt) { select_stmt_ = select_stmt; }
  void set_physical_operator(std::unique_ptr<PhysicalOperator> physical_operator) { physical_operator_ = std::move(physical_operator); }
  void set_view_definition(const std::string &view_definition) { view_definition_ = view_definition; }
  void set_view_updatable(bool is_view_updatable) { is_view_updatable_ = is_view_updatable; }
  SelectStmt *select_stmt() const { return select_stmt_; }
  PhysicalOperator *physical_operator() const { return physical_operator_.get(); }
  bool is_view_updatable() const { return is_view_updatable_; }

  const std::vector<FieldMeta> &query_fields_meta() const { return query_fields_meta_; }
  void set_query_fields(const std::vector<FieldMeta> &query_fields_meta) { query_fields_meta_ = query_fields_meta; }

  bool has_duplicate_column_name();


  static RC            create(Db *db, CreateViewSqlNode &create_view, Stmt *&stmt);

private:
  std::string view_name_;
  std::string view_definition_;
  std::vector<std::string> attrs_name_;
  
  SelectStmt *select_stmt_ = nullptr;
  std::unique_ptr<PhysicalOperator> physical_operator_ = nullptr;
  std::vector<FieldMeta> query_fields_meta_;
  
  bool is_view_updatable_ = false;
};