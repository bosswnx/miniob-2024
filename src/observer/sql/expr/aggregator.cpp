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
// Created by Wangyunlai on 2024/05/29.
//

#include "sql/expr/aggregator.h"
#include "common/log/log.h"

RC SumAggregator::accumulate(const Value &value)
{
  if (value_.attr_type() == AttrType::UNDEFINED) {
    value_ = value;
    return RC::SUCCESS;
  }
  
  ASSERT(value.attr_type() == value_.attr_type(), "type mismatch. value type: %s, value_.type: %s", 
        attr_type_to_string(value.attr_type()), attr_type_to_string(value_.attr_type()));

  RC rc = Value::add(value, value_, value_);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to add value. rc=%s", strrc(rc));
    return rc;
  }
  return RC::SUCCESS;
}

RC SumAggregator::evaluate(Value& result)
{
  result = value_;
  return RC::SUCCESS;
}

RC AvgAggregator::accumulate(const Value &value)
{
  if (value_.attr_type() == AttrType::UNDEFINED) {
    value_ = value;
    count_ = 1;
    return RC::SUCCESS;
  }

  ASSERT(value.attr_type() == value_.attr_type(), "type mismatch. value type: %s, value_.type: %s", 
        attr_type_to_string(value.attr_type()), attr_type_to_string(value_.attr_type()));

  Value sum_value;
  RC    rc = Value::add(value, value_, sum_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to add value. rc=%s", strrc(rc));
    return rc;
  }
  value_ = sum_value;
  count_++;
  return RC::SUCCESS;
}

RC AvgAggregator::evaluate(Value &result)
{
  if (count_ == 0) {
    result.set_int(0);
    return RC::SUCCESS;
  }
  Value divisor;
  divisor.set_int(count_);
  result.set_float(0);
  Value::divide(value_, divisor, result);
  return RC::SUCCESS;
}

RC CountAggregator::accumulate(const Value &value)
{
  count_++;
  return RC::SUCCESS;
}

RC CountAggregator::evaluate(Value &result)
{
  result.set_int(count_);
  return RC::SUCCESS;
}

RC MaxAggregator::accumulate(const Value &value)
{
  if (value_.attr_type() == AttrType::UNDEFINED) {
    value_ = value;
    return RC::SUCCESS;
  }

  Value result;
  RC    rc = Value::max(value, value_, result);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to max value. rc=%s", strrc(rc));
    return rc;
  }
  value_ = result;
  return RC::SUCCESS;
}

RC MaxAggregator::evaluate(Value &result)
{
  result = value_;
  return RC::SUCCESS;
}

RC MinAggregator::accumulate(const Value &value)
{
  Value result;
  RC    rc = Value::min(value, value_, result);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to min value. rc=%s", strrc(rc));
    return rc;
  }
  value_ = result;
  return RC::SUCCESS;
}

RC MinAggregator::evaluate(Value &result)
{
  result = value_;
  return RC::SUCCESS;
}
