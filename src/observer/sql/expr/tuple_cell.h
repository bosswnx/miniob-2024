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
// Created by WangYunlai on 2022/6/7.
//

#pragma once

#include "storage/field/field_meta.h"
#include <iostream>

class TupleCellSpec final
{
public:
  TupleCellSpec() = default;
  TupleCellSpec(const char *table_name, const char *field_name, const char *alias = nullptr);
  explicit TupleCellSpec(const char *alias);
  explicit TupleCellSpec(const std::string &alias);

  const char *table_name() const { return table_name_.c_str(); }
  const char *field_name() const { return field_name_.c_str(); }
  const char *alias() const { return alias_.c_str(); }
  std::string table_alias() const { return table_alias_; }

  bool equals(const TupleCellSpec &other) const
  {
    return table_name_ == other.table_name_ && field_name_ == other.field_name_ && alias_ == other.alias_;
  }

  void set_table_name(const char *table_name) { table_name_ = table_name; }
  void set_field_name(const char *field_name) { field_name_ = field_name; }
  void set_alias(const char *alias) { alias_ = alias; }
  void set_table_alias(std::string table_alias) { table_alias_ = table_alias; }

private:
  std::string table_name_;
  std::string field_name_;
  std::string alias_;
  std::string table_alias_;
};
