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
// Created by Wangyunlai on 2021/5/12.
//

#pragma once

#include "common/rc.h"
#include "common/lang/string.h"

class TableMeta;
class FieldMeta;

namespace Json {
class Value;
}  // namespace Json

/**
 * @brief 描述一个索引
 * @ingroup Index
 * @details 一个索引包含了表的哪些字段，索引的名称等。
 * 如果以后实现了多种类型的索引，还需要记录索引的类型，对应类型的一些元数据等
 */
class IndexMeta
{
public:
  IndexMeta() = default;

  RC init(const std::string &name, const std::vector<const FieldMeta *> &field_metas, bool is_unique);
  RC init(const std::string &name, const std::vector<FieldMeta> &field_metas, bool is_unique);

public:
  const std::string            &name() const;
  const std::vector<FieldMeta> &field_metas() const;
  bool                          is_unique() const { return is_unique_; }

  void desc(ostream &os) const;

public:
  void      to_json(Json::Value &json_value) const;
  static RC from_json(const TableMeta &table, const Json::Value &json_value, IndexMeta &index);

  bool has_field(const std::string &field_name) const;

protected:
  std::string            name_;         // index's name
  std::vector<FieldMeta> field_metas_;  // field's metas
  bool                   is_unique_;    // whether is unique index
};
