/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details.
By Nelson Boss 2024/10/13
*/

#include "common/lang/comparator.h"
#include "common/log/log.h"
#include "common/type/date_type.h"
#include "common/value.h"

int DateType::compare(const Value &left, const Value &right) const
{
  ASSERT(left.attr_type() == AttrType::DATES && right.attr_type() == AttrType::DATES, "invalid type");
  return common::compare_int((void *)&left.value_.int_value_, (void *)&right.value_.int_value_);
}

RC DateType::max(const Value &left, const Value &right, Value &result) const
{
  int cmp = common::compare_int((void *)&left.value_.int_value_, (void *)&right.value_.int_value_);
  if (cmp > 0) {
    result.set_date(left.value_.int_value_);
  } else {
    result.set_date(right.value_.int_value_);
  }
  return RC::SUCCESS;
}

RC DateType::min(const Value &left, const Value &right, Value &result) const
{
  int cmp = common::compare_int((void *)&left.value_.int_value_, (void *)&right.value_.int_value_);
  if (cmp < 0) {
    result.set_date(left.value_.int_value_);
  } else {
    result.set_date(right.value_.int_value_);
  }
  return RC::SUCCESS;
}

RC DateType::set_value_from_str(Value &val, const string &data) const
{
  std::string date = data.substr(0, 4) + data.substr(5, 2) + data.substr(8, 2);
  val.set_date(atoi(date.c_str()));
  return RC::SUCCESS;
}

RC DateType::cast_to(const Value &val, AttrType type, Value &result) const
{
  // 日期类型不支持转换
  switch (type) {
    default: return RC::UNIMPLEMENTED;
  }
  return RC::SUCCESS;
}

int DateType::cast_cost(AttrType type)
{
  if (type == AttrType::DATES) {
    return 0;
  }
  return INT32_MAX;
}

// 将 YYYYMMDD 格式的时间转换为 YYYY-MM-DD 格式
RC DateType::to_string(const Value &val, string &result) const
{
  int year  = val.value_.int_value_ / 10000;
  int month = (val.value_.int_value_ / 100) % 100;
  int day   = val.value_.int_value_ % 100;
  result    = std::to_string(year) + "-" + (month < 10 ? "0" : "") + std::to_string(month) + "-" + (day < 10 ? "0" : "") + std::to_string(day);
  return RC::SUCCESS;
}