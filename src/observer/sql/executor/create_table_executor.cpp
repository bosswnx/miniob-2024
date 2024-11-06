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

#include "sql/executor/create_table_executor.h"

#include "common/log/log.h"
#include "event/session_event.h"
#include "event/sql_event.h"
#include "session/session.h"
#include "sql/stmt/create_table_stmt.h"
#include "storage/db/db.h"

RC CreateTableExecutor::execute(SQLStageEvent *sql_event)
{
  Stmt    *stmt    = sql_event->stmt();
  Session *session = sql_event->session_event()->session();
  ASSERT(stmt->type() == StmtType::CREATE_TABLE,
      "create table executor can not run this command: %d",
      static_cast<int>(stmt->type()));

  CreateTableStmt *create_table_stmt = static_cast<CreateTableStmt *>(stmt);

  const char *table_name = create_table_stmt->table_name().c_str();

  RC rc = RC::SUCCESS;

  if (create_table_stmt->physical_operator() != nullptr) {
    size_t size_ = 0;
    // create-table-select
    if (!create_table_stmt->attr_infos().empty()) {
      // 指定了字段信息
      rc = session->get_current_db()->create_table(table_name, create_table_stmt->attr_infos(), create_table_stmt->storage_format());
      size_ = create_table_stmt->attr_infos().size();
    } else {
      std::vector<AttrInfoSqlNode> attr_infos;
      for (size_t i = 0; i < create_table_stmt->query_fields_meta().size(); ++i) {
        AttrInfoSqlNode attr_info;
        attr_info.nullable = create_table_stmt->query_fields_meta()[i].nullable();
        attr_info.name = create_table_stmt->query_fields_meta()[i].name();
        attr_info.type = create_table_stmt->query_fields_meta()[i].type();
        if (attr_info.type == AttrType::VECTORS) {
          attr_info.arr_len = create_table_stmt->query_fields_meta()[i].vector_dim();
        } else if (attr_info.type == AttrType::CHARS) {
          attr_info.arr_len = create_table_stmt->query_fields_meta()[i].len();
        } else {
          attr_info.arr_len = 1;
        }
        attr_infos.push_back(attr_info);
      }
      size_ = attr_infos.size();
      rc = session->get_current_db()->create_table(table_name, attr_infos, create_table_stmt->storage_format());
    }

    // 开始插入数据
    Trx *trx = session->current_trx();
    rc = create_table_stmt->physical_operator()->open(session->current_trx());
    Table *table_ = session->get_current_db()->find_table(table_name);
    while (create_table_stmt->physical_operator()->next() == RC::SUCCESS) {
      Tuple *tuple = create_table_stmt->physical_operator()->current_tuple();
      Record record;
      std::vector<Value> values_;
      for (size_t i = 0; i < size_; ++i) {
        Value value;
        tuple->cell_at(i, value);
        values_.push_back(value);
      }
      rc = table_->make_record(static_cast<int>(values_.size()), values_.data(), record);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to make record. rc=%s", strrc(rc));
        return rc;
      }
      rc = trx->insert_record(table_, record);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to insert record by transaction. rc=%s", strrc(rc));
      }
    }
    
  } else {
    // normally create table
    rc = session->get_current_db()->create_table(table_name, create_table_stmt->attr_infos(), create_table_stmt->storage_format());
  }

  return rc;
}