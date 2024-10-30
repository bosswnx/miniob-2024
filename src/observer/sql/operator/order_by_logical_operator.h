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
// Created by Nelson Boss on 2024/10/29.
//

#pragma once

#include <memory>
#include <vector>

#include "sql/expr/expression.h"
#include "sql/operator/logical_operator.h"

/**
 * @brief order by 表示排序运算
 * @ingroup LogicalOperator
 * @details 从表中获取数据后，可能需要过滤，投影，连接等等。
 */
class OrderByLogicalOperator : public LogicalOperator
{
public:
  OrderByLogicalOperator(std::vector<std::unique_ptr<Expression>> &&expressions,
                         std::vector<bool> &&order_by_descs);
  virtual ~OrderByLogicalOperator() = default;

  LogicalOperatorType type() const override { return LogicalOperatorType::ORDER_BY; }

  std::vector<std::unique_ptr<Expression>>       &expressions() { return expressions_; }
  std::vector<bool> &order_by_descs() { return order_by_descs_; }

private:
  std::vector<bool> order_by_descs_;
};
