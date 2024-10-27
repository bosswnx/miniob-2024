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
// Created by WangYunlai on 2022/12/30.
//

#include "sql/operator/join_physical_operator.h"

NestedLoopJoinPhysicalOperator::NestedLoopJoinPhysicalOperator() {}

RC NestedLoopJoinPhysicalOperator::open(Trx *trx)
{
  if (children_.size() != 2) {
    LOG_WARN("nlj operator should have 2 children");
    return RC::INTERNAL;
  }

  RC rc         = RC::SUCCESS;
  left_         = children_[0].get();
  right_        = children_[1].get();
  right_closed_ = true;

  rc = left_->open(trx);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  rc = right_->open(trx);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  trx_ = trx;
  return rc;
}

RC NestedLoopJoinPhysicalOperator::load_left_block()
{
  // left_block_.clear();
  left_block_.clear();
  RC rc = RC::SUCCESS;
  while ((rc = left_->next()) == RC::SUCCESS) {
    if (left_->type() == PhysicalOperatorType::TABLE_SCAN) {
      left_block_.push_back(new RowTuple(static_cast<RowTuple &>(*left_->current_tuple())));
    } else {
      // join 
      left_block_.push_back(new JoinedTuple(static_cast<JoinedTuple &>(*left_->current_tuple())));
    }

    if (left_block_.size() >= left_block_size_) {
      break;
    }
  }
  if (left_block_.empty()) {
    // EOF
    return RC::RECORD_EOF;
  }
  left_block_index_ = 0;
  return rc == RC::RECORD_EOF ? RC::SUCCESS : rc;
}

RC NestedLoopJoinPhysicalOperator::next()
{
  RC rc = RC::SUCCESS;
  if (left_tuple_ == nullptr) {
    // initial load
    rc = load_left_block();
    if (rc != RC::SUCCESS) {
      return rc;
    }
  }
  if (right_tuple_ == nullptr) {
    // initial load
    rc = right_next();
    if (rc != RC::SUCCESS) {
      return rc;
    }
  }
  
  if (left_block_.empty()) {
    return RC::RECORD_EOF;
  }
  if (left_block_index_ >= left_block_.size()) {
    rc = right_next();
    if (rc != RC::SUCCESS) {
      if (rc == RC::RECORD_EOF) {
        rc = load_left_block();
        if (rc != RC::SUCCESS) {
          return rc;
        }
        // reload right
        if (!right_closed_) {
          rc = right_->close();
          if (rc != RC::SUCCESS) {
            return rc;
          }
          right_closed_ = true;
        }
        rc = right_->open(trx_);
        if (rc != RC::SUCCESS) {
          return rc;
        }
        right_closed_ = false;
        rc            = right_next();
        if (rc != RC::SUCCESS) {
          return rc;
        }
      } else {
        return rc;
      }
    }
    left_block_index_ = 0;
  }
  left_tuple_ = left_block_[left_block_index_++];
  joined_tuple_.set_left(left_tuple_);
  return rc;
}

RC NestedLoopJoinPhysicalOperator::close()
{
  RC rc = left_->close();
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to close left oper. rc=%s", strrc(rc));
  }

  if (!right_closed_) {
    rc = right_->close();
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to close right oper. rc=%s", strrc(rc));
    } else {
      right_closed_ = true;
    }
  }
  return rc;
}

Tuple *NestedLoopJoinPhysicalOperator::current_tuple() { return &joined_tuple_; }

RC NestedLoopJoinPhysicalOperator::right_next()
{
  RC rc = RC::SUCCESS;
  rc    = right_->next();
  if (rc != RC::SUCCESS) {
    return rc;
  }

  right_tuple_ = right_->current_tuple();
  joined_tuple_.set_right(right_tuple_);
  return rc;
}
