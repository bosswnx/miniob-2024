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
#include <iomanip>
#include "common/lang/comparator.h"
#include "common/log/log.h"
#include "common/type/vector_type.h"
#include "common/value.h"

int VectorType::compare(const Value &left_value, const Value &right_value) const
{
  ASSERT(left_value.attr_type() == AttrType::VECTORS && right_value.attr_type() == AttrType::VECTORS, "invalid type");
  // 以字典序比较两个 vector<float> 的大小
  const VectorData left  = left_value.get_vector();
  const VectorData right = right_value.get_vector();

  size_t idx = 0;
  while (idx < left.dim && idx < right.dim) {
    if (left.vector[idx] < right.vector[idx]) {
      return -1;
    } else if (left.vector[idx] > right.vector[idx]) {
      return 1;
    }
    idx++;
  }
  if (idx < left.dim) {
    return 1;
  }
  if (idx < right.dim) {
    return -1;
  }
  return 0;
}

RC VectorType::add(const Value &left_value, const Value &right_right, Value &result) const
{
  const VectorData left  = left_value.get_vector();
  const VectorData right = right_right.get_vector();

  if (left.dim != right.dim) {
    return RC::INVALID_ARGUMENT;  // TODO(soulter): 需要支持广播吗
  }
  VectorData res    = {.dim = left.dim};
  auto       buffer = new float[left.dim];
  for (size_t i = 0; i < left.dim; ++i) {
    buffer[i] = left.vector[i] + right.vector[i];
  }
  res.vector = buffer;
  result.set_vector(res, true);
  return RC::SUCCESS;
}

RC VectorType::subtract(const Value &left_value, const Value &right_right, Value &result) const
{
  const VectorData left  = left_value.get_vector();
  const VectorData right = right_right.get_vector();

  if (left.dim != right.dim) {
    return RC::INVALID_ARGUMENT;  // TODO(soulter): 需要支持广播吗
  }
  VectorData res    = {.dim = left.dim};
  auto       buffer = new float[left.dim];
  for (size_t i = 0; i < left.dim; ++i) {
    buffer[i] = left.vector[i] - right.vector[i];
  }
  res.vector = buffer;
  result.set_vector(res, true);
  return RC::SUCCESS;
}

RC VectorType::multiply(const Value &left_value, const Value &right_right, Value &result) const
{
  const VectorData left  = left_value.get_vector();
  const VectorData right = right_right.get_vector();

  if (left.dim != right.dim) {
    return RC::INVALID_ARGUMENT;  // TODO(soulter): 需要支持广播吗
  }
  VectorData res    = {.dim = left.dim};
  auto       buffer = new float[left.dim];
  for (size_t i = 0; i < left.dim; ++i) {
    buffer[i] = left.vector[i] * right.vector[i];
  }
  res.vector = buffer;
  result.set_vector(res, true);
  return RC::SUCCESS;
}

RC VectorType::max(const Value &left_value, const Value &right_value, Value &result) const
{
  int cmp = compare(left_value, right_value);
  if (cmp > 0) {
    result.set_vector(left_value.value_.vector_value_);
  } else {
    result.set_vector(right_value.get_vector());
  }
  return RC::SUCCESS;
}

RC VectorType::min(const Value &left, const Value &right, Value &result) const
{
  int cmp = compare(left, right);
  if (cmp < 0) {
    result.set_vector(left.get_vector());
  } else {
    result.set_vector(right.get_vector());
  }
  return RC::SUCCESS;
}

RC VectorType::set_value_from_str(Value &val, const string &data) const
{
  val.set_vector(data.c_str());
  return RC::SUCCESS;
}

RC VectorType::cast_to(const Value &val, AttrType type, Value &result) const
{
  switch (type) {
    default: return RC::UNIMPLEMENTED;
  }
  return RC::SUCCESS;
}

int VectorType::cast_cost(AttrType type)
{
  if (type == AttrType::DATES) {
    return 0;
  }
  return INT32_MAX;
}

RC VectorType::to_string(const Value &val, string &result) const
{
  stringstream ss;
  VectorData   vec = val.get_vector();
  ss << "[";
  for (size_t i = 0; i < vec.dim; i++) {
    // if (vec.vector[i] != vec.vector[i]) {
    //     break;
    // }
    // 最多保留两位小数，去掉末尾的0
    // ss << vec[i];
    ss << std::fixed << std::setprecision(2) << vec.vector[i];
    string temp = ss.str();
    temp.erase(temp.find_last_not_of('0') + 1, string::npos); // 去掉末尾的0
    if (temp.back() == '.') {
        temp.pop_back();
    }
    ss.str("");
    ss.clear();
    ss << temp;

    if (i != vec.dim - 1) {
      ss << ",";
    }
  }
  ss << "]";
  result = ss.str();
  return RC::SUCCESS;
}