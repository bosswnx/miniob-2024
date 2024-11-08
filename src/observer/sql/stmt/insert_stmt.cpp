/* Copyright (c) 2021OceanBase and/or its affiliates. All rights reserved.
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

#include "sql/stmt/insert_stmt.h"
#include "common/log/log.h"
#include "storage/db/db.h"
#include "storage/table/table.h"

InsertStmt::InsertStmt(Table *table, const Value *values, int value_amount)
    : table_(table), values_(values), value_amount_(value_amount)
{}

RC InsertStmt::create(Db *db, InsertSqlNode &inserts, Stmt *&stmt)
{
  const char *table_name = inserts.relation_name.c_str();
  if (nullptr == db || nullptr == table_name || inserts.values.empty()) {
    LOG_WARN("invalid argument. db=%p, table_name=%p, value_num=%d",
        db, table_name, static_cast<int>(inserts.values.size()));
    return RC::INVALID_ARGUMENT;
  }

  // check whether the table exists
  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  
  if (table->is_view()) {
    auto *view = static_cast<View *>(table);
    // 带聚合等的 View 不可插入
    if (!view->is_updatable()) {
      LOG_WARN("the target table(view) of the INSERT is not insertable-into");
      return RC::INVALID_ARGUMENT;
    }

    // 有 join 的，如果没有 attrs_name，不可插入~
    if (inserts.attrs_name.empty() && view->base_tables().size() > 1) {
      LOG_WARN("Can not insert into join view without fields list");
      return RC::INVALID_ARGUMENT;
    }

    // 有 join 的，不可以同时插入多个表的数据
    std::string test_table_name;
    for (auto &attr_name : inserts.attrs_name) {
      if (test_table_name != "") {
        if (view->find_base_table_name(attr_name) != test_table_name) {
          LOG_WARN("Can not insert into join view with fields from multiple tables");
          return RC::INVALID_ARGUMENT;
        }
      } else {
        test_table_name = view->find_base_table_name(attr_name);
      }
    }

  }

  // check the fields number
  Value           *values     = inserts.values.data();
  const int        value_num  = static_cast<int>(inserts.values.size());
  const TableMeta &table_meta = table->table_meta();
  const int        field_num  = table_meta.field_num() - table_meta.sys_field_num();

  bool is_view_and_field_list_exist = false;
  if (!table->is_view() || (table->is_view() && inserts.attrs_name.empty())) {
    // 不是视图，或者是没有指定 field list 的视图插入操作。
    if (field_num != value_num) {
      LOG_WARN("schema mismatch. value num=%d, field num in schema=%d", value_num, field_num);
      return RC::SCHEMA_FIELD_MISSING;
    }
  } else {
    // 指定了 field list 的视图的视图插入操作，需要检查 attrs_name 是否和 value_num 匹配
    if (inserts.attrs_name.size() != value_num) {
      LOG_WARN("schema mismatch. value num=%d, field num in schema=%d", value_num, field_num);
      return RC::SCHEMA_FIELD_MISSING;
    }
    is_view_and_field_list_exist = true;
  }

  // TODO(soulter): 当制定了attr_name并且是视图并且字段长度不等于表的长时，这里会报错。
  // 检查每列的类型和nullable 是否匹配(在执行阶段还有检查）
  // 注意：sys_field 是为了 mvcc 等设计的系统隐藏字段，不检查
  if (is_view_and_field_list_exist) {
    for (int i = 0; i < inserts.attrs_name.size(); ++i) {
      bool found = false;
      for (auto field_meta : *table->table_meta().field_metas()) {
        if (field_meta.name() == inserts.attrs_name[i]) {
          found = true;

          if (!values[i].is_null() && values[i].attr_type() != field_meta.type()) {
            // 类型不匹配时尝试转型，chars 可以转成 text
            Value to_value;
            RC    rc = Value::cast_to(values[i], field_meta.type(), to_value);
            if (OB_FAIL(rc)) {
              LOG_WARN("value doesn't match: %s != %s", attr_type_to_string(values[i].attr_type()), attr_type_to_string(field_meta.type()));
              return RC::SCHEMA_FIELD_TYPE_MISMATCH;
            }
            values[i] = to_value;
          }

          if (values[i].is_null() && !field_meta.nullable()) {
            LOG_WARN("value of column %s cannot be null", field_meta.name());
            return RC::SCHEMA_FIELD_TYPE_MISMATCH;
          }

          // 检查 VECTOR 的维度是否完全一致
          if (field_meta.type() == AttrType::VECTORS &&
              static_cast<size_t>(values[i].length()) != field_meta.vector_dim() * sizeof(float)) {
            LOG_WARN("vector length exceeds limit: %d != %d", values[i].length()/4, field_meta.vector_dim());
            return RC::INVALID_ARGUMENT;
          }

          break;
        }
      }
      if (!found) {
        LOG_WARN("field %s you specified in the field list is not found in table %s", inserts.attrs_name[i].c_str(), table->name());
        return RC::SCHEMA_FIELD_MISSING;
      }
      
    } 
  } else {
    for (int i = table_meta.sys_field_num(); i < table_meta.field_num(); i++) {
      int value_idx = i - table_meta.sys_field_num();
      if (!values[value_idx].is_null() && values[value_idx].attr_type() != table_meta.field(i)->type()) {
        // 类型不匹配时尝试转型，chars 可以转成 text
        Value to_value;
        RC    rc = Value::cast_to(values[value_idx], table_meta.field(i)->type(), to_value);
        if (OB_FAIL(rc)) {
          LOG_WARN("value doesn't match: %s != %s", attr_type_to_string(values[value_idx].attr_type()), attr_type_to_string(table_meta.field(i)->type()));
          return RC::SCHEMA_FIELD_TYPE_MISMATCH;
        }
        values[value_idx] = to_value;
      }
      if (values[value_idx].is_null() && !table_meta.field(i)->nullable()) {
        LOG_WARN("value of column %s cannot be null", table_meta.field(i)->name());
        return RC::SCHEMA_FIELD_TYPE_MISMATCH;
      }
      // 检查 VECTOR 的维度是否完全一致
      if (table_meta.field(i)->type() == AttrType::VECTORS &&
          static_cast<size_t>(values[value_idx].length()) != table_meta.field(i)->vector_dim() * sizeof(float)) {
        LOG_WARN("vector length exceeds limit: %d != %d", values[value_idx].length()/4, table_meta.field(i)->vector_dim());
        return RC::INVALID_ARGUMENT;
      }
    }
  }
  // everything alright
  auto insert_stmt = new InsertStmt(table, values, value_num);
  insert_stmt->set_attrs_name(inserts.attrs_name);
  stmt = insert_stmt;
  return RC::SUCCESS;
}
