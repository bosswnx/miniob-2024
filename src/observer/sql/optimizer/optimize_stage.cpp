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

#include "optimize_stage.h"

#include "common/conf/ini.h"
#include "common/io/io.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "event/session_event.h"
#include "event/sql_event.h"
#include "sql/operator/logical_operator.h"
#include "sql/stmt/create_table_stmt.h"
#include "sql/stmt/create_view_stmt.h"
#include "sql/stmt/stmt.h"
#include "storage/db/db.h"

using namespace std;
using namespace common;

RC OptimizeStage::handle_request(SQLStageEvent *sql_event)
{
  unique_ptr<LogicalOperator> logical_operator;

  RC rc = create_logical_plan(sql_event, logical_operator);
  if (rc != RC::SUCCESS) {
    if (rc != RC::UNIMPLEMENTED) {
      LOG_WARN("failed to create logical plan. rc=%s", strrc(rc));
    }
    return rc;
  }

  ASSERT(logical_operator, "logical operator is null");

  rc = optimize(logical_operator);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to optimize plan. rc=%s", strrc(rc));
    return rc;
  }

  unique_ptr<PhysicalOperator> physical_operator;
  rc = generate_physical_plan(logical_operator, physical_operator, sql_event->session_event()->session());
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to generate physical plan. rc=%s", strrc(rc));
    return rc;
  }

  sql_event->set_operator(std::move(physical_operator));

  // create table set physical oper
  Stmt *stmt = sql_event->stmt();
  if (stmt->type() == StmtType::CREATE_TABLE) {
    auto *create_table_stmt = static_cast<CreateTableStmt *>(stmt);
    if (create_table_stmt->select_stmt() != nullptr) {
      create_table_stmt->set_physical_operator(std::move(sql_event->physical_operator()));
    }
  } else if (stmt->type() == StmtType::CREATE_VIEW) {
    // create view set physical oper
    auto *create_view_stmt = static_cast<CreateViewStmt *>(stmt);
    if (create_view_stmt->select_stmt() != nullptr) {
      create_view_stmt->set_physical_operator(std::move(sql_event->physical_operator()));
    }
  }

  handle_view_request(sql_event);

  return rc;
}


RC OptimizeStage::handle_view_request(SQLStageEvent *sql_event)
{
  // 创建 view 的描述的逻辑计划。
  RC rc = RC::SUCCESS;
  
  for (size_t i = 0; i < sql_event->stmt_views().size(); ++i) {
    unique_ptr<LogicalOperator> logical_operator;
    rc = create_logical_plan_view(sql_event, logical_operator, i);
    if (rc != RC::SUCCESS) {
      if (rc != RC::UNIMPLEMENTED) {
        LOG_WARN("failed to create logical plan. rc=%s", strrc(rc));
      }
      return rc;
    }

    ASSERT(logical_operator, "logical operator is null");

    rc = optimize(logical_operator);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to optimize plan. rc=%s", strrc(rc));
      return rc;
    }

    unique_ptr<PhysicalOperator> physical_operator;

    rc = generate_physical_plan(logical_operator, physical_operator, sql_event->session_event()->session());
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to generate physical plan. rc=%s", strrc(rc));
      return rc;
    }
    auto *view = sql_event->session_event()->session()->get_current_db()->find_view(sql_event->views_name()[i].c_str());
    view->set_operator(std::move(physical_operator)); // 🤓
  }

  return rc;
}

RC OptimizeStage::optimize(unique_ptr<LogicalOperator> &oper)
{
  RC rc = RC::SUCCESS;
  rc    = rewrite(oper);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("failed to rewrite logical plan. rc=%s", strrc(rc));
    return rc;
  }
  return rc;
}

RC OptimizeStage::generate_physical_plan(
    unique_ptr<LogicalOperator> &logical_operator, unique_ptr<PhysicalOperator> &physical_operator, Session *session)
{
  RC rc = RC::SUCCESS;
  if (session->get_execution_mode() == ExecutionMode::CHUNK_ITERATOR && LogicalOperator::can_generate_vectorized_operator(logical_operator->type())) {
    LOG_INFO("use chunk iterator");
    session->set_used_chunk_mode(true);
    rc    = physical_plan_generator_.create_vec(*logical_operator, physical_operator);
  } else {
    LOG_INFO("use tuple iterator");
    session->set_used_chunk_mode(false);
    rc = physical_plan_generator_.create(*logical_operator, physical_operator);
  }
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create physical operator. rc=%s", strrc(rc));
  }
  return rc;
}

RC OptimizeStage::rewrite(unique_ptr<LogicalOperator> &logical_operator)
{
  RC rc = RC::SUCCESS;

  bool change_made = false;
  do {
    change_made = false;
    rc          = rewriter_.rewrite(logical_operator, change_made);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to do expression rewrite on logical plan. rc=%s", strrc(rc));
      return rc;
    }
  } while (change_made);

  return rc;
}

RC OptimizeStage::create_logical_plan(SQLStageEvent *sql_event, unique_ptr<LogicalOperator> &logical_operator)
{
  Stmt *stmt = sql_event->stmt();
  if (nullptr == stmt) {
    return RC::UNIMPLEMENTED;
  }

  return logical_plan_generator_.create(stmt, logical_operator);
}

RC OptimizeStage::create_logical_plan_view(SQLStageEvent *sql_event, unique_ptr<LogicalOperator> &logical_operator, size_t stmt_idx)
{
  Stmt *stmt = sql_event->stmt_views()[stmt_idx];
  if (nullptr == stmt) {
    return RC::UNIMPLEMENTED;
  }

  return logical_plan_generator_.create(stmt, logical_operator);
}