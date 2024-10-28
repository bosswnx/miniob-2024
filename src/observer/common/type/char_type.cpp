/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "common/lang/comparator.h"
#include "common/log/log.h"
#include "common/type/char_type.h"
#include "common/value.h"
#include <math.h>

int CharType::compare(const Value &left, const Value &right) const
{
  // left 是 string。right 是数字的情况，需要将 string 转为数字再比较
  if (left.attr_type() == AttrType::CHARS && (right.attr_type() == AttrType::INTS || right.attr_type() == AttrType::FLOATS)) {
    float this_value = common::db_str_to_float(left.value_.pointer_value_);
    float another_value = right.attr_type() == AttrType::INTS ? right.value_.int_value_ : right.value_.float_value_;
    return common::compare_float((void *)&this_value, (void *)&another_value);
  }
  
  ASSERT(left.attr_type() == AttrType::CHARS && right.attr_type() == AttrType::CHARS, "invalid type");
  return common::compare_string(
      (void *)left.value_.pointer_value_, left.length_, (void *)right.value_.pointer_value_, right.length_);
}

RC CharType::max(const Value &left, const Value &right, Value &result) const
{
  int cmp = common::compare_string(
      (void *)left.value_.pointer_value_, left.length_, (void *)right.value_.pointer_value_, right.length_);
  if (cmp < 0) {
    result.set_string(right.value_.pointer_value_, right.length_);
  } else {
    result.set_string(left.value_.pointer_value_, left.length_);
  }
  return RC::SUCCESS;
}

RC CharType::min(const Value &left, const Value &right, Value &result) const
{
  int cmp = common::compare_string(
      (void *)left.value_.pointer_value_, left.length_, (void *)right.value_.pointer_value_, right.length_);
  if (cmp < 0) {
    result.set_string(left.value_.pointer_value_, left.length_);
  } else {
    result.set_string(right.value_.pointer_value_, right.length_);
  }
  return RC::SUCCESS;
}

RC CharType::set_value_from_str(Value &val, const string &data) const
{
  val.set_string(data.c_str());
  return RC::SUCCESS;
}

RC CharType::cast_to(const Value &val, AttrType type, Value &result) const
{
  switch (type) {
    case AttrType::INTS: {
      int to = int(common::db_str_to_float(val.value_.pointer_value_));
      result.set_int(to);
      break;
    }
    case AttrType::FLOATS: {
      float to = common::db_str_to_float(val.value_.pointer_value_);
      result.set_float(to);
      break;
    }
    default: return RC::UNIMPLEMENTED;
  }
  return RC::SUCCESS;
}

int CharType::cast_cost(AttrType type)
{
  if (type == AttrType::CHARS) {
    return 0;
  }
  return INT32_MAX;
}

RC CharType::to_string(const Value &val, string &result) const
{
  stringstream ss;
  ss << val.value_.pointer_value_;
  result = ss.str();
  return RC::SUCCESS;
}