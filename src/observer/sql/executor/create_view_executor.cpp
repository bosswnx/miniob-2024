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

#include "sql/executor/create_view_executor.h"

#include "common/log/log.h"
#include "event/session_event.h"
#include "event/sql_event.h"
#include "session/session.h"
#include "sql/stmt/create_view_stmt.h"
#include "sql/stmt/select_stmt.h"
#include "storage/db/db.h"

void CreateViewExecutor::init_sys_view_table_attr_infos(std::vector<AttrInfoSqlNode> &attr_infos) {
    // 暂时这样写
    AttrInfoSqlNode attr_info;
    attr_info.name = "view_name";
    attr_info.type = AttrType::CHARS;
    attr_info.arr_len = 64; // same as MYSQL
    attr_info.nullable = false;
    attr_infos.push_back(attr_info);

    attr_info.name = "view_definition";
    attr_info.type = AttrType::CHARS;
    attr_info.arr_len = 1024;
    attr_info.nullable = false;
    attr_infos.push_back(attr_info);

    attr_info.name = "is_updatable";
    attr_info.type = AttrType::INTS;
    attr_info.arr_len = 1;
    attr_info.nullable = false;
    attr_infos.push_back(attr_info);

    attr_info.name = "attrs_name";
    attr_info.type = AttrType::CHARS;
    attr_info.arr_len = 512;
    attr_info.nullable = false;
    attr_infos.push_back(attr_info);
}

void CreateViewExecutor::make_view_values(std::vector<Value> &values, 
const std::vector<std::string> &attrs_name,
const std::string &view_name, 
const std::string &view_definition,
bool is_updatable) {
    Value value;
    value.set_string(view_name.c_str());
    values.push_back(value);

    value.set_string(view_definition.c_str());
    values.push_back(value);

    value.set_int(is_updatable);
    values.push_back(value);

    std::string attrs_name_str;
    for (const auto &attr : attrs_name) {
        attrs_name_str += attr;
        attrs_name_str += ",";
    }
    value.set_string(attrs_name_str.c_str());
    values.push_back(value);
}

RC CreateViewExecutor::execute(SQLStageEvent *sql_event) {
    Stmt *stmt = sql_event->stmt();
    Session *session = sql_event->session_event()->session();

    ASSERT(stmt->type() == StmtType::CREATE_VIEW,
    "create table executor can not run this command: %d",
    static_cast<int>(stmt->type()));
    
    auto *create_view_stmt = static_cast<CreateViewStmt *>(stmt);
    
    if (create_view_stmt->physical_operator() == nullptr) {
        LOG_PANIC("create view: select physical operator is null");
        return RC::INVALID_ARGUMENT;
    }

    RC rc = RC::SUCCESS;

    // auto select_stmt = create_view_stmt->select_stmt();
    // bool is_updatable = check_is_updatable(select_stmt);
    bool is_updatable = create_view_stmt->is_view_updatable();

    // 走一遍 select 的物理算子，确保 select 语句没问题才能创建视图
    rc = create_view_stmt->physical_operator()->open(session->current_trx());
    while (create_view_stmt->physical_operator()->next() == RC::SUCCESS);
    if (rc != RC::SUCCESS && rc != RC::RECORD_EOF) {
        LOG_ERROR("create view: failed to execute select phy oper. rc=%d", rc);
        return rc;
    }

    // 创建视图
    const char *view_name = create_view_stmt->view_name().c_str();
    const char *view_definition = create_view_stmt->view_definition().c_str();

    // 检查名字重复性
    if (session->get_current_db()->find_view(view_name) != nullptr || 
       session->get_current_db()->find_table(view_name) != nullptr) {
        LOG_WARN("create view: view name already exists. view_name=%s", view_name);
        return RC::INVALID_ARGUMENT;
    }

    // 检查 __miniob_views__ 表是否存在
    Table *table = session->get_current_db()->find_table("__miniob_views__");
    if (table == nullptr) {
        // 建表
        std::vector<AttrInfoSqlNode> attr_infos;
        init_sys_view_table_attr_infos(attr_infos);
        rc = session->get_current_db()->create_table("__miniob_views__", attr_infos, StorageFormat::ROW_FORMAT);
        if (rc != RC::SUCCESS) {
            LOG_WARN("failed to create table __miniob_views__. rc=%d", rc);
            return rc;
        }
        table = session->get_current_db()->find_table("__miniob_views__");
    }

    Trx *trx = session->current_trx();
    Record record;
    std::vector<Value> values;
    make_view_values(values, create_view_stmt->attrs_name(), view_name, view_definition, is_updatable);
    table->make_record(values.size(), values.data(), record);
    if (rc != RC::SUCCESS) {
        LOG_WARN("failed to make record. rc=%s", strrc(rc));
        return rc;
    }
    rc = trx->insert_record(table, record);
    if (rc != RC::SUCCESS) {
        LOG_WARN("failed to insert record by transaction. rc=%s", strrc(rc));
    }

    rc = session->get_current_db()->add_view(view_name, create_view_stmt->attrs_name(), view_definition, is_updatable);

    if (rc != RC::SUCCESS) {
        LOG_WARN("failed to add view. rc=%d", rc);
    }

    return rc;
}