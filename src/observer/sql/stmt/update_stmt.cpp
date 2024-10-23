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

#include "sql/stmt/update_stmt.h"
#include "sql/parser/expression_binder.h"
#include "storage/db/db.h"
#include "storage/table/table.h"
#include "storage/table/table_meta.h"

UpdateStmt::UpdateStmt(
    Table *table, std::vector<FieldMeta> attrs, std::vector<std::unique_ptr<Expression>> exprs, FilterStmt *stmt)
    : table_(table), field_metas_(std::move(attrs)), exprs_(std::move(exprs)), filter_stmt_(stmt)
{}

RC UpdateStmt::create(Db *db, const UpdateSqlNode &update, Stmt *&stmt)
{
  // 检查表名是否存在
  Table *table = db->find_table(update.relation_name.c_str());
  if (table == nullptr) {
    LOG_WARN("table %s doesn't exit", update.relation_name.c_str());
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  TableMeta     meta = table->table_meta();
  BinderContext context;
  context.add_table(table);
  ExpressionBinder               binder(context);
  vector<unique_ptr<Expression>> bound_expressions;
  std::vector<FieldMeta>         field_metas;
  for (const auto &[attr, expr] : update.update_infos) {
    auto field_meta = meta.field(attr.c_str());
    if (field_meta == nullptr) {
      LOG_WARN("field %s not found in table %s", field_meta->name(), table->name());
      return RC::SCHEMA_FIELD_NOT_EXIST;
    }
    field_metas.push_back(*field_meta);
    std::unique_ptr<Expression> exprp(expr);
    RC                          rc = binder.bind_expression(exprp, bound_expressions);
    if (OB_FAIL(rc)) {
      LOG_INFO("bind expression failed. rc=%s", strrc(rc));
      return rc;
    }
  }
  std::unordered_map<std::string, Table *> table_map   = {{update.relation_name, table}};
  FilterStmt                              *filter_stmt = nullptr;
  RC rc = FilterStmt::create(db, table, &table_map, update.conditions.data(), update.conditions.size(), filter_stmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("cannot construct filter stmt");
    return rc;
  }
  stmt = new UpdateStmt(table, std::move(field_metas), std::move(bound_expressions), filter_stmt);
  return RC::SUCCESS;
}
