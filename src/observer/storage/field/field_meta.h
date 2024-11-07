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
// Created by Meiyi & Wangyunlai on 2021/5/12.
//

#pragma once

#include "common/rc.h"
#include "common/lang/string.h"
#include "sql/parser/parse_defs.h"

namespace Json {
class Value;
}  // namespace Json

/**
 * @brief 字段元数据
 *
 */
class FieldMeta
{
public:
  FieldMeta();
  FieldMeta(const char *name, AttrType attr_type, int attr_offset, int attr_len, bool visible, int field_id,
      bool nullable = false);
  ~FieldMeta() = default;

  FieldMeta(const FieldMeta &field) {
    name_        = field.name_;
    attr_type_   = field.attr_type_;
    attr_offset_ = field.attr_offset_;
    attr_len_    = field.attr_len_;
    visible_     = field.visible_;
    field_id_    = field.field_id_;
    nullable_    = field.nullable_;
    vector_dim_  = field.vector_dim_;

    table_name_  = field.table_name_;
  }

  RC init(const char *name, AttrType attr_type, int attr_offset, int attr_len, bool visible, int field_id,
      bool nullable = false, int vector_dim = 0);

public:
  const char *name() const;
  AttrType    type() const;
  int         offset() const;
  int         len() const;
  int         vector_dim() const;
  bool        visible() const;
  int         field_id() const;
  bool        nullable() const;

public:
  void desc(ostream &os) const;

public:
  void      to_json(Json::Value &json_value) const;
  static RC from_json(const Json::Value &json_value, FieldMeta &field);

  // view 中，需要识别到某个 Field 属于哪个表
  std::string table_name_;

  void set_name(const char *name) { name_ = name; }

protected:
  string   name_;
  AttrType attr_type_;
  int      attr_offset_;
  int      attr_len_;  // 表示字段在行内所占的长度
  bool     visible_;
  int      field_id_;  // 从零递增的 id
  bool     nullable_;
  int      vector_dim_;  // 只在 vector 字段使用
};
