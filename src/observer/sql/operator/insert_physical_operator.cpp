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
// Created by WangYunlai on 2021/6/9.
//

#include "sql/operator/insert_physical_operator.h"
#include "sql/stmt/insert_stmt.h"
#include "storage/table/table.h"
#include "storage/table/view.h"
#include "storage/trx/trx.h"

using namespace std;

InsertPhysicalOperator::InsertPhysicalOperator(Table *table, vector<Value> &&values)
    : table_(table), values_(std::move(values))
{}

RC InsertPhysicalOperator::open(Trx *trx)
{
  RC rc = RC::SUCCESS;
  Record record;

  if (table_->is_view()) {
    // 重新编排 Values
    auto *view = static_cast<View *>(table_);
    auto base_tables = view->base_tables();
    unordered_map<string, std::vector<Value>> base_table_values;
    unordered_map<string, Table *> base_table_map;
    for (auto *base_table : base_tables) {
      // 对于某个 base table（可能有join，会有多个base table）
      std::vector<Value> base_table_value;
      for (auto &field : *base_table->table_meta().field_metas()) {
        // 遍历这个 base table 的所有 field
        bool found = false;
        Value value;
        size_t idx = 0;
        string view_field_name;
        for (auto &view_field : *view->table_meta().field_metas()) {
          // 遍历视图的所有 field
          // if (field.name() == view_field.name()) { // 犯错误了
          if (view->attrs_name().empty()) {
            view_field_name = view_field.name();
          } else {
            view_field_name = view->find_base_table_field_name(view_field.name());
          }


          // 检查这个视图当中的 field_name 在不在 insert 用户指定的 attrs_name 里面
          if (!attrs_name_.empty()) {
            if (find(attrs_name_.begin(), attrs_name_.end(), view_field.name()) == attrs_name_.end()) {
              // 如果不在 attrs_name 里面，就不用插入了
              continue;
            }
          }


          if (strcmp(field.name(), view_field_name.c_str()) == 0) {
            // 如果找到一个视图的 field 和 base table 的 field 名字相同
            found = true;
            value = values_[idx];
            break;
          }
          ++idx;
        }
        if (!found) {
          // 如果没有找到，判断可不可为null
          if (!field.nullable()) {
            LOG_WARN("when handing view insert, field %s is not nullable, so we cannot insert", field.name()); 
            return RC::INVALID_ARGUMENT;
          }
          value.set_null();
        }
        base_table_value.push_back(value);
      }
      base_table_map[base_table->name()] = base_table;
      base_table_values[base_table->name()] = base_table_value;
    }
    
    // 插入
    for (auto &base_table : base_tables) {
      auto &values = base_table_values[base_table->name()];
      Record record;
      rc = base_table->make_record(static_cast<int>(values.size()), values.data(), record);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to make record. rc=%s", strrc(rc));
        return rc;
      }
      rc = trx->insert_record(base_table, record);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to insert record by transaction. rc=%s", strrc(rc));
        return rc;
      }
    }

  } else {
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

  return rc;
}

RC InsertPhysicalOperator::next() { return RC::RECORD_EOF; }

RC InsertPhysicalOperator::close() { return RC::SUCCESS; }
