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
// Created by Wangyunlai on 2022/5/22.
//

#include "sql/stmt/stmt.h"
#include "common/log/log.h"
#include "sql/stmt/calc_stmt.h"
#include "sql/stmt/create_index_stmt.h"
#include "sql/stmt/create_table_stmt.h"
#include "sql/stmt/delete_stmt.h"
#include "sql/stmt/desc_table_stmt.h"
#include "sql/stmt/exit_stmt.h"
#include "sql/stmt/explain_stmt.h"
#include "sql/stmt/help_stmt.h"
#include "sql/stmt/insert_stmt.h"
#include "sql/stmt/load_data_stmt.h"
#include "sql/stmt/select_stmt.h"
#include "sql/stmt/set_variable_stmt.h"
#include "sql/stmt/show_tables_stmt.h"
#include "sql/stmt/trx_begin_stmt.h"
#include "sql/stmt/trx_end_stmt.h"
#include "sql/stmt/drop_table_stmt.h"
#include "sql/stmt/update_stmt.h"
#include "storage/db/db.h"

class Db;

bool stmt_type_ddl(StmtType type)
{
  switch (type) {
    case StmtType::CREATE_TABLE:
    case StmtType::DROP_TABLE:
    case StmtType::DROP_INDEX:
    case StmtType::CREATE_INDEX: {
      return true;
    }
    default: {
      return false;
    }
  }
}
/**
 * 根据抽象语法树的节点类型, 创建对应的语句对象
 */
RC Stmt::create_stmt(Db *db, ParsedSqlNode &sql_node, Stmt *&stmt)
{
  stmt = nullptr;

  switch (sql_node.flag) {
    case SCF_INSERT: {
      return InsertStmt::create(db, sql_node.insertion, stmt);
    }
    case SCF_DELETE: {
      return DeleteStmt::create(db, sql_node.deletion, stmt);
    }
    case SCF_SELECT: {
      return SelectStmt::create(db, sql_node.selection, stmt);
    }

    case SCF_EXPLAIN: {
      return ExplainStmt::create(db, sql_node.explain, stmt);
    }

    case SCF_CREATE_INDEX: {
      return CreateIndexStmt::create(db, sql_node.create_index, stmt);
    }

    case SCF_CREATE_TABLE: {
      return CreateTableStmt::create(db, sql_node.create_table, stmt);
    }

    case SCF_DESC_TABLE: {
      return DescTableStmt::create(db, sql_node.desc_table, stmt);
    }

    case SCF_HELP: {
      return HelpStmt::create(stmt);
    }

    case SCF_SHOW_TABLES: {
      return ShowTablesStmt::create(db, stmt);
    }

    case SCF_BEGIN: {
      return TrxBeginStmt::create(stmt);
    }

    case SCF_COMMIT:
    case SCF_ROLLBACK: {
      return TrxEndStmt::create(sql_node.flag, stmt);
    }

    case SCF_EXIT: {
      return ExitStmt::create(stmt);
    }

    case SCF_SET_VARIABLE: {
      return SetVariableStmt::create(sql_node.set_variable, stmt);
    }

    case SCF_LOAD_DATA: {
      return LoadDataStmt::create(db, sql_node.load_data, stmt);
    }

    case SCF_CALC: {
      return CalcStmt::create(sql_node.calc, stmt);
    }

    case SCF_DROP_TABLE: {
      return DropTableStmt::create(db, sql_node.drop_table, stmt);
    }
    case SCF_UPDATE: {
      return UpdateStmt::create(db, sql_node.update, stmt);
    }

    default: {
      LOG_INFO("Command::type %d doesn't need to create statement.", sql_node.flag);
    } break;
  }
  return RC::UNIMPLEMENTED;
}

RC Stmt::check_sub_select_legal(Db *db, ParsedSqlNode *sub_select)
{
  // 这个方法主要是检查子查询的合法性：子查询的查询的属性只能有一个。
  FieldExpr *field_expr = nullptr;
  StarExpr  *star_expr  = nullptr;
  for (auto &expr : sub_select->selection.expressions) {
    if (field_expr != nullptr) {
      // 当左子查询的属性不止一个时，报错
      LOG_WARN("invalid subquery attributes. It should be only one");
      return RC::INVALID_ARGUMENT;
    }
    if (expr->type() == ExprType::FIELD) {
      field_expr = static_cast<FieldExpr *>(expr.get());
    } else if (expr->type() == ExprType::STAR) {
      star_expr = static_cast<StarExpr *>(expr.get());
    }
  }
  if (field_expr != nullptr && star_expr != nullptr) {
    LOG_WARN("star_expr and field_expr cannot be used together in subquery");
    return RC::INVALID_ARGUMENT;
  }

  if (star_expr != nullptr) {
    // 如果是 *，需要先获得 table，然后看其中的 fields 的大小是不是 1，如果不是，报错
    int fields_num = 0;
    for (size_t j = 0; j < sub_select->selection.relations.size(); ++j) {
      const char *table_name = sub_select->selection.relations[j].name.c_str();
      if (nullptr == table_name) {
        LOG_WARN("invalid argument. relation name is null. index=%d", j);
        return RC::INVALID_ARGUMENT;
      }
      Table *table = db->find_table(table_name);
      if (nullptr == table) {
        LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
        return RC::SCHEMA_TABLE_NOT_EXIST;
      }
      fields_num += table->table_meta().field_num();
    }
    if (fields_num != 1) {
      LOG_WARN("invalid subquery attributes");
      return RC::INVALID_ARGUMENT;
    }
  }
  return RC::SUCCESS;
}
