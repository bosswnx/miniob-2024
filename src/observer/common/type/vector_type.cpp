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

int VectorType::compare(const Value &left, const Value &right) const
{
  ASSERT(left.attr_type() == AttrType::VECTORS && right.attr_type() == AttrType::VECTORS, "invalid type");
  // 以字典序比较两个 vector<float> 的大小
  vector<float> left_ = left.get_vector();
  vector<float> right_ = right.get_vector();

  int idx = 0;
  while (idx < left_.size() && idx < right_.size()) {
    if (left_[idx] < right_[idx]) {
      return -1;
    } else if (left_[idx] > right_[idx]) {
      return 1;
    }
    idx++;
  }
  if (idx < left_.size()) return 1;
  else if (idx < right_.size()) return -1;
  return 0;
}

RC VectorType::add(const Value &left, const Value &right, Value &result) const {
  if (left.length_ != right.length_) return RC::INVALID_ARGUMENT; // TODO(soulter): 需要支持广播吗
  vector<float> res(left.length_);
  vector<float> left_ = left.get_vector();
  vector<float> right_ = right.get_vector();
  for (int i=0; i<left.length_; ++i) {
    res[i] = left_[i] + right_[i];
  }
  result.set_vector(res);
  return RC::SUCCESS;
}

RC VectorType::subtract(const Value &left, const Value &right, Value &result) const {
  if (left.length_ != right.length_) return RC::INVALID_ARGUMENT; // TODO(soulter): 需要支持广播吗
  vector<float> res(left.length_);
  vector<float> left_ = left.get_vector();
  vector<float> right_ = right.get_vector();
  for (int i=0; i<left.length_; ++i) {
    res[i] = left_[i] - right_[i];
  }
  result.set_vector(res);
  return RC::SUCCESS;
}

RC VectorType::multiply(const Value &left, const Value &right, Value &result) const {
  if (left.length_ != right.length_) return RC::INVALID_ARGUMENT; // TODO(soulter): 需要支持广播吗
  vector<float> res(left.length_);
  vector<float> left_ = left.get_vector();
  vector<float> right_ = right.get_vector();
  for (int i=0; i<left.length_; ++i) {
    res[i] = left_[i] * right_[i];
  }
  result.set_vector(res);
  return RC::SUCCESS;
}

RC VectorType::max(const Value &left, const Value &right, Value &result) const
{
  int cmp = compare(left, right);
  if (cmp > 0) {
    result.set_vector(left.get_vector());
  } else {
    result.set_vector(right.get_vector());
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
  vector<float> vec = val.get_vector();
  ss << "[";
  for (int i = 0; i < vec.size(); i++) {
    if (vec[i] != vec[i]) {
        break;
    }
    // 最多保留两位小数，去掉末尾的0
    // ss << vec[i];
    ss << std::fixed << std::setprecision(2) << vec[i];
    string temp = ss.str();
    temp.erase(temp.find_last_not_of('0') + 1, string::npos); // 去掉末尾的0
    if (temp.back() == '.') {
        temp.pop_back();
    }
    ss.str("");
    ss.clear();
    ss << temp;

    if (i != vec.size() - 1) {
        ss << ",";
    }
  }
  ss << "]";
  result = ss.str();
  return RC::SUCCESS;
}