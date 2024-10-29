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

#include "sql/expr/tuple.h"
#include "sql/operator/physical_operator.h"
#include "sql/expr/expression_tuple.h"

/**
 * @brief 排序物理算子
 * @ingroup PhysicalOperator
 */
class OrderByPhysicalOperator : public PhysicalOperator
{
public:
  OrderByPhysicalOperator(std::vector<std::unique_ptr<Expression>> &&expressions,
                          std::vector<bool> &&order_by_descs);

  virtual ~OrderByPhysicalOperator() = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::ORDER_BY; }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  int cell_num() const { return tuples_[0]->cell_num(); }

  Tuple *current_tuple() override;

  RC tuple_schema(TupleSchema &schema) const override;

private:
  std::vector<std::unique_ptr<Expression>>     order_by_exprs_;  // 排序依赖的表达式
  std::vector<bool>                            order_by_descs_;  // 排序方向
  std::vector<std::unique_ptr<ValueListTuple>> tuples_;          // 存储排序后的元组
  size_t                                       tuple_idx_ = 0;       // 当前返回的元组索引，从 1 开始，索引下标的时候要减 1
};
