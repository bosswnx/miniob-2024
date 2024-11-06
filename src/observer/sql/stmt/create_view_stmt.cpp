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

RC CreateViewStmt::create(Db *db, CreateViewSqlNode &create_view, Stmt *&stmt) {
  // create table select
  SelectStmt *select_stmt = nullptr;
  Stmt *stmt_ = nullptr;
  if (create_view.sub_select != nullptr) {
    // create table select
    RC rc = Stmt::create_stmt(db, *create_view.sub_select, stmt_);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to create stmt. rc=%d:%s", rc, strrc(rc));
      return rc;
    }
    // cast to SelectStmt
    select_stmt = static_cast<SelectStmt *>(stmt_);
  }
  stmt = new CreateViewStmt(create_view.view_name);
  
  if (create_view.sub_select != nullptr) {
    auto *create_view_stmt = static_cast<CreateViewStmt *>(stmt);
    create_view_stmt->set_select_stmt(select_stmt);
    create_view_stmt->set_query_fields(select_stmt->get_query_fields());
    create_view_stmt->set_view_definition(create_view.description);
  }

  sql_debug("create table statement: table name %s", create_view.view_name.c_str());
  
  return RC::SUCCESS;
}