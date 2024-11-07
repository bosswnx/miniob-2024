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
// Created by WangYunlai on 2021/6/9.
//

#include "sql/operator/table_scan_physical_operator.h"
#include "event/sql_debug.h"
#include "storage/table/table.h"
#include "storage/table/view.h"

using namespace std;

RC TableScanPhysicalOperator::open(Trx *trx)
{
  RC rc = RC::SUCCESS;
  if (table_->is_view()) {
    auto *view = static_cast<View *>(table_);
    rc = view->get_record_scanner(record_scanner_view_, trx, mode_);;
  } else {
    rc = table_->get_record_scanner(record_scanner_, trx, mode_);
  }
  if (rc == RC::SUCCESS) {
    tuple_.set_schema(table_, table_->table_meta().field_metas());
  }
  trx_ = trx;
  return rc;
}

RC TableScanPhysicalOperator::next()
{
  RC rc = RC::SUCCESS;

  if (table_->is_view()) {
    bool filter_result = false;
    while (OB_SUCC(rc = record_scanner_view_.next_tuple())) {
      auto t_tuple = record_scanner_view_.current_tuple();
      LOG_TRACE("table scan oper got a tuple.");
      ValueListTuple value_list_tuple_ = ValueListTuple();

      ValueListTuple::make(*t_tuple, value_list_tuple_, table_->name());

      // 重新创建 Record，为了转换成 RowTuple
      table_->make_record(value_list_tuple_.cell_num(), value_list_tuple_.cells().data(), current_record_);
      
      rc = filter(value_list_tuple_, filter_result);
      if (rc != RC::SUCCESS) {
        LOG_TRACE("record filtered failed=%s", strrc(rc));
        return rc;
      }

      if (filter_result) {
        sql_debug("get a tuple(view): %s", value_list_tuple_.to_string().c_str());
        break;
      } else {
        sql_debug("a tuple is filtered(view): %s", value_list_tuple_.to_string().c_str());
      }
    }
  } else {
    bool filter_result = false;
    while (OB_SUCC(rc = record_scanner_.next(current_record_))) {
      LOG_TRACE("got a record. rid=%s", current_record_.rid().to_string().c_str());
      
      tuple_.set_record(&current_record_);
      rc = filter(tuple_, filter_result);
      if (rc != RC::SUCCESS) {
        LOG_TRACE("record filtered failed=%s", strrc(rc));
        return rc;
      }

      if (filter_result) {
        sql_debug("get a tuple: %s", tuple_.to_string().c_str());
        break;
      } else {
        sql_debug("a tuple is filtered: %s", tuple_.to_string().c_str());
      }
    }
  }

  
  return rc;
}

RC TableScanPhysicalOperator::close() { 
  return record_scanner_.close_scan(); 
}

Tuple *TableScanPhysicalOperator::current_tuple()
{
  // if (table_->is_view()) {
  //   return &value_list_tuple_;
  // }
  tuple_.set_record(&current_record_);
  return &tuple_;
}

string TableScanPhysicalOperator::param() const { return table_->name(); }

void TableScanPhysicalOperator::set_predicates(vector<unique_ptr<Expression>> &&exprs)
{
  predicates_ = std::move(exprs);
}

RC TableScanPhysicalOperator::filter(Tuple &tuple, bool &result)
{
  RC    rc = RC::SUCCESS;
  Value value;

  bool has_true = false;
  for (unique_ptr<Expression> &expr : predicates_) {
    rc = expr->get_value(tuple, value);
    if (rc != RC::SUCCESS) {
      return rc;
    }

    bool tmp_result = value.get_boolean();
    if (!tmp_result) {
      // and, 有一个为false，整个表达式为false
      if (!is_or_conjunction) {
        result = false;
        return rc;
      }
    } else {
      // or, 有一个为true，整个表达式为true
      if (is_or_conjunction) {
        result = true;
        return rc;
      }
      has_true = true;
    }
  }
  // 都为true，并且是and
  // 都为false，并且是or

  if (is_or_conjunction) {
    result = has_true;
  } else {
    result = true;
  }

  return rc;
}
