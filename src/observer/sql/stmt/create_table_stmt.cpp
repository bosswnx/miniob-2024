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

#include "common/log/log.h"
#include "sql/stmt/create_table_stmt.h"
#include "event/sql_debug.h"

#include "sql/parser/parse_defs.h"
#include "sql/stmt/select_stmt.h"

RC CreateTableStmt::create(Db *db, CreateTableSqlNode &create_table, Stmt *&stmt)
{
  StorageFormat storage_format = StorageFormat::UNKNOWN_FORMAT;
  if (create_table.storage_format.length() == 0) {
    storage_format = StorageFormat::ROW_FORMAT;
  } else {
    storage_format = get_storage_format(create_table.storage_format.c_str());
  }
  if (storage_format == StorageFormat::UNKNOWN_FORMAT) {
    return RC::INVALID_ARGUMENT;
  }

  // create table select
  SelectStmt *select_stmt = nullptr;
  Stmt *stmt_ = nullptr;
  if (create_table.sub_select != nullptr) {
    // create table select
    RC rc = Stmt::create_stmt(db, *create_table.sub_select, stmt_);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to create stmt. rc=%d:%s", rc, strrc(rc));
      return rc;
    }
    // cast to SelectStmt
    select_stmt = static_cast<SelectStmt *>(stmt_);
  }

  // vector的特殊之处: vector(1024)和char(1024)具有相同的形式，在语法分析阶段没有区分开来
  for (auto &attr : create_table.attr_infos) {
    if (attr.type == AttrType::VECTORS) {
      attr.dim     = attr.arr_len;
      attr.arr_len = 1;
    }
  }
  stmt = new CreateTableStmt(create_table.relation_name, create_table.attr_infos, storage_format);

  if (create_table.sub_select != nullptr) {
    CreateTableStmt *create_table_stmt = static_cast<CreateTableStmt *>(stmt);
    create_table_stmt->set_select_stmt(select_stmt);
    create_table_stmt->set_query_fields(select_stmt->get_query_fields());
  }

  sql_debug("create table statement: table name %s", create_table.relation_name.c_str());
  return RC::SUCCESS;
}

StorageFormat CreateTableStmt::get_storage_format(const char *format_str) {
  StorageFormat format = StorageFormat::UNKNOWN_FORMAT;
  if (0 == strcasecmp(format_str, "ROW")) {
    format = StorageFormat::ROW_FORMAT;
  } else if (0 == strcasecmp(format_str, "PAX")) {
    format = StorageFormat::PAX_FORMAT;
  } else {
    format = StorageFormat::UNKNOWN_FORMAT;
  }
  return format;
}
