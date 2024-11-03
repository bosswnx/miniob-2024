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
// Created by Wangyunlai on 2023/6/13.
//

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "sql/stmt/stmt.h"
#include "common/types.h"
#include "sql/operator/physical_operator.h"
#include "sql/parser/parse_defs.h"

class Db;

/**
 * @brief 表示创建表的语句
 * @ingroup Statement
 * @details 虽然解析成了stmt，但是与原始的SQL解析后的数据也差不多
 */
class CreateTableStmt : public Stmt
{
public:
  CreateTableStmt(
      const std::string &table_name, const std::vector<AttrInfoSqlNode> &attr_infos, StorageFormat storage_format)
      : table_name_(table_name), attr_infos_(attr_infos), storage_format_(storage_format)
  {}
  virtual ~CreateTableStmt() = default;

  StmtType type() const override { return StmtType::CREATE_TABLE; }

  const std::string                  &table_name() const { return table_name_; }
  const std::vector<AttrInfoSqlNode> &attr_infos() const { return attr_infos_; }
  const StorageFormat                 storage_format() const { return storage_format_; }

  void set_select_stmt(SelectStmt *select_stmt) { select_stmt_ = select_stmt; }
  void set_physical_operator(std::unique_ptr<PhysicalOperator> physical_operator) { physical_operator_ = std::move(physical_operator); }
  SelectStmt *select_stmt() const { return select_stmt_; }
  PhysicalOperator *physical_operator() const { return physical_operator_.get(); }

  const std::vector<FieldMeta> &query_fields_meta() const { return query_fields_meta_; }
  void set_query_fields(const std::vector<FieldMeta> &query_fields_meta) { query_fields_meta_ = query_fields_meta; }

  static RC            create(Db *db, CreateTableSqlNode &create_table, Stmt *&stmt);
  static StorageFormat get_storage_format(const char *format_str);

private:
  std::string                  table_name_;
  std::vector<AttrInfoSqlNode> attr_infos_;
  StorageFormat                storage_format_;
  
  SelectStmt *select_stmt_ = nullptr;
  std::unique_ptr<PhysicalOperator> physical_operator_ = nullptr;
  std::vector<FieldMeta> query_fields_meta_;
};