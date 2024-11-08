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
// Created by Longda on 2021/4/13.
//

#include <string.h>
#include <string>

#include "resolve_stage.h"

#include "common/conf/ini.h"
#include "common/io/io.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "event/session_event.h"
#include "event/sql_event.h"
#include "session/session.h"
#include "sql/stmt/stmt.h"
#include "sql/stmt/select_stmt.h"
#include "sql/stmt/create_view_stmt.h"
#include "storage/db/db.h"

using namespace common;

RC ResolveStage::handle_request(SQLStageEvent *sql_event)
{
  RC            rc            = RC::SUCCESS;
  SessionEvent *session_event = sql_event->session_event();
  SqlResult    *sql_result    = session_event->sql_result();

  Db *db = session_event->session()->get_current_db();
  if (nullptr == db) {
    LOG_ERROR("cannot find current db");
    rc = RC::SCHEMA_DB_NOT_EXIST;
    sql_result->set_return_code(rc);
    sql_result->set_state_string("no db selected");
    return rc;
  }


  // 创建视图的 Select Stmt
  auto &sql_node_views = sql_event->sql_node_views();
  for (size_t i=0; i<sql_event->sql_node_views().size(); ++i) {
    Stmt *view_stmt = nullptr;
    rc = Stmt::create_stmt(db, *sql_node_views[i], view_stmt);
    if (rc != RC::SUCCESS && rc != RC::UNIMPLEMENTED) {
      LOG_WARN("failed to create view stmt. rc=%d:%s", rc, strrc(rc));
      sql_result->set_return_code(rc);
      return rc;
    }
    sql_event->add_view_stmt(view_stmt);
    auto *view = db->find_view(sql_event->views_name()[i].c_str());
    auto select_stmt_ = static_cast<SelectStmt *>(view_stmt);
    view->init_table_meta(select_stmt_->get_query_fields()); // 初始化 table_meta
    view->set_base_tables(select_stmt_->tables()); // for update, insert
  }
  // 到这里，视图和视图的描述的 Stmt 都已经创建好了

  ParsedSqlNode *sql_node = sql_event->sql_node().get();
  Stmt          *stmt     = nullptr;

  rc = Stmt::create_stmt(db, *sql_node, stmt);
  if (rc != RC::SUCCESS && rc != RC::UNIMPLEMENTED) {
    LOG_WARN("failed to create stmt. rc=%d:%s", rc, strrc(rc));
    sql_result->set_return_code(rc);
    return rc;
  }

  sql_event->set_stmt(stmt);

  return rc;
}
