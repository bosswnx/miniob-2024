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

#include "sql/stmt/create_view_stmt.h"
#include "common/log/log.h"
#include "sql/stmt/create_table_stmt.h"
#include "event/sql_debug.h"

#include "sql/parser/parse_defs.h"
#include "sql/stmt/select_stmt.h"
#include <cstddef>

bool check_is_updatable(SelectStmt *select_stmt) {
    // 检查是否有聚合函数, 算数表达式
    if (select_stmt->has_special_queries()) {
        return false;
    }
    return true;
}

RC CreateViewStmt::create(Db *db, CreateViewSqlNode &create_view, Stmt *&stmt) {
  // create table select
  SelectStmt *select_stmt = nullptr;
  Stmt *stmt_ = nullptr;

  if (create_view.sub_select == nullptr) {
    return RC::INVALID_ARGUMENT;
  }

  RC rc = Stmt::create_stmt(db, *create_view.sub_select, stmt_);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create stmt. rc=%d:%s", rc, strrc(rc));
    return rc;
  }
  // cast to SelectStmt
  select_stmt = static_cast<SelectStmt *>(stmt_);

  auto query_fields = select_stmt->get_query_fields();

  if (create_view.attrs_name.size() != 0 && query_fields.size() != create_view.attrs_name.size()) {
    LOG_WARN("select query expr num count doesn't match attr count");
    return RC::INVALID_ARGUMENT;
  }

  // 检查是否有重复的字段名
  unordered_map<string, std::nullptr_t> attr_name_map;
  for (auto &attr_name : create_view.attrs_name) {
    if (attr_name_map.find(attr_name) != attr_name_map.end()) {
      LOG_WARN("duplicate column name in view definition");
      return RC::INVALID_ARGUMENT;
    }
    attr_name_map[attr_name] = nullptr;
  }

  stmt = new CreateViewStmt(create_view.view_name, create_view.attrs_name);
  
  auto *create_view_stmt = static_cast<CreateViewStmt *>(stmt);
  // 解析到 is_updatable
  // 不可更新的判断条件为：
  // 1. 聚合函数、Join
  create_view_stmt->set_view_updatable(check_is_updatable(select_stmt));
  create_view_stmt->set_select_stmt(select_stmt);
  create_view_stmt->set_query_fields(query_fields);
  // 检查 duplicate column name
  if (create_view_stmt->has_duplicate_column_name()) {
    LOG_WARN("duplicate column name in view definition(ERROR 1060)");
    return RC::INVALID_ARGUMENT;
  }
  create_view_stmt->set_view_definition(create_view.description);

  sql_debug("create view statement: view name %s", create_view.view_name.c_str());
  
  return RC::SUCCESS;
}


bool CreateViewStmt::has_duplicate_column_name() {
    std::unordered_map<std::string, int> column_name_map;
    for (auto &field : query_fields_meta_) {
        if (column_name_map.find(field.name()) != column_name_map.end()) {
            return true;
        }
        column_name_map[field.name()] = 1;
    }
    return false;
}