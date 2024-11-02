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
// Created by Wangyunlai.wyl on 2021/5/18.
//

#include "storage/index/index_meta.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "storage/field/field_meta.h"
#include "storage/table/table_meta.h"
#include "json/json.h"

const static Json::StaticString FIELD_NAME("name");
const static Json::StaticString FIELD_FIELD_NAME("field_name");
const static Json::StaticString FIELD_IS_UNIQUE("is_unique");
RC IndexMeta::init(const std::string &name, const std::vector<const FieldMeta *> &field_metas, bool is_unique)
{
  if (name.empty()) {
    LOG_ERROR("Failed to init index, name is empty.");
    return RC::INVALID_ARGUMENT;
  }
  name_ = name;
  for (const FieldMeta *field_meta : field_metas) {
    field_metas_.push_back(*field_meta);
  }
  is_unique_ = is_unique;
  return RC::SUCCESS;
}

RC IndexMeta::init(const std::string &name, const std::vector<FieldMeta> &field_metas, bool is_unique)
{
  if (name.empty()) {
    LOG_ERROR("Failed to init index, name is empty.");
    return RC::INVALID_ARGUMENT;
  }
  name_        = name;
  field_metas_ = field_metas;
  is_unique_   = is_unique;
  return RC::SUCCESS;
}

void IndexMeta::to_json(Json::Value &json_value) const
{
  json_value[FIELD_NAME] = name_;
  json_value[FIELD_IS_UNIQUE] = is_unique_;
  // 创建一个 JSON 数组来存储所有字段名
  Json::Value field_metas;
  for (const FieldMeta &field_meta : field_metas_) {
    Json::Value field_meta_json;
    field_meta.to_json(field_meta_json);
    field_metas.append(field_meta_json);
  }
  json_value[FIELD_FIELD_NAME] = field_metas;
}

RC IndexMeta::from_json(const TableMeta &table, const Json::Value &json_value, IndexMeta &index)
{
  RC                 rc          = RC::SUCCESS;
  const Json::Value &name_value  = json_value[FIELD_NAME];
  const Json::Value &field_value = json_value[FIELD_FIELD_NAME];
  const Json::Value &is_unique_value = json_value[FIELD_IS_UNIQUE];
  if (!name_value.isString()) {
    LOG_ERROR("Index name is not a string. json value=%s", name_value.toStyledString().c_str());
    return RC::INTERNAL;
  }

  std::vector<FieldMeta> field_metas;
  for (const Json::Value &field_meta_value : field_value) {
    FieldMeta field_meta;
    rc = FieldMeta::from_json(field_meta_value, field_meta);
    if (rc != RC::SUCCESS) {
      LOG_ERROR("Failed to deserialize field meta. json value=%s", field_meta_value.toStyledString().c_str());
      return rc;
    }
    field_metas.push_back(field_meta);
  }

  return index.init(name_value.asCString(), field_metas, is_unique_value.asBool());
}

const std::string &IndexMeta::name() const { return name_; }

const std::vector<FieldMeta> &IndexMeta::field_metas() const { return field_metas_; }

void IndexMeta::desc(ostream &os) const
{
  os << "index name=" << name_ << ", fields=[";
  for (size_t i = 0; i < field_metas_.size(); i++) {
    if (i > 0) {
      os << ", ";
    }
    os << field_metas_[i].name();
  }
  os << "]";
}

bool IndexMeta::has_field(const std::string &field_name) const
{
  for (const FieldMeta &field_meta : field_metas_) {
    if (field_meta.name() == field_name) {
      return true;
    }
  }
  return false;
}
