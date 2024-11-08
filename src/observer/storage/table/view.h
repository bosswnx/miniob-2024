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
#pragma once

#include <string>
#include <storage/table/table.h>
#include <sql/operator/physical_operator.h>

class RecordPhysicalOperatorScanner;

class View : public Table {
 public:
  virtual ~View() = default;
  View(std::string view_name, std::vector<std::string> attrs_name, std::string view_definition, bool is_updatable, int32_t view_id)
      : view_name_(std::move(view_name)),
        view_definition_(std::move(view_definition)),
        attrs_name_(std::move(attrs_name)),
        is_updatable_(is_updatable),
        view_id_(view_id) {set_view(true);}
  
  const std::string &view_name() const { return view_name_; }
  const std::string &view_definition() const { return view_definition_; }
  const std::vector<std::string> &attrs_name() const { return attrs_name_; }
  const bool is_updatable() const { return is_updatable_; }

  void init_table_meta(const vector<FieldMeta> &fields);

  void set_operator(std::unique_ptr<PhysicalOperator> oper) { operator_ = std::move(oper); }

  RC get_record_scanner(RecordPhysicalOperatorScanner &scanner, Trx *trx, ReadWriteMode mode);

  void set_base_tables(const std::vector<Table *> &tables) { base_tables_ = tables; }
  
  const std::vector<Table *> &base_tables() const { return base_tables_; }

  // std::unordered_map<std::string, std::string> &field_base_table_name_map() { return field_base_table_name; }
  std::string find_base_table_name(const std::string &field_name) {
    if (field_base_table_name.find(field_name) != field_base_table_name.end()) {
      return field_base_table_name[field_name];
    }
    return "";
  }

  std::string find_base_table_field_name(const std::string &attr_name) {
    if (attr_name_2_base_table_field_name.find(attr_name) != attr_name_2_base_table_field_name.end()) {
      return attr_name_2_base_table_field_name[attr_name];
    }
    return "";
  }
  
 private:
  std::string view_name_;
  std::string view_definition_;
  std::vector<std::string> attrs_name_;
  bool is_updatable_;
  int32_t view_id_;

  std::unique_ptr<PhysicalOperator> operator_;

  // base table
  std::vector<Table *> base_tables_;

  // 维护某个视图的普通字段（不包括聚合等不普通的）属于哪个表
  std::unordered_map<std::string, std::string> field_base_table_name;

  // 维护视图的字段名和基表的字段名的映射关系
  std::unordered_map<std::string, std::string> attr_name_2_base_table_field_name;
};