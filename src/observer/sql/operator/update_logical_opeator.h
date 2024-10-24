#pragma once

#include "update_logical_opeator.h"
#include "sql/operator/logical_operator.h"


class UpdateLogicalOperator : public LogicalOperator
{
public:
  UpdateLogicalOperator(Table *table, std::vector<FieldMeta> field_metas, std::vector<std::unique_ptr<Expression>> expr):table_(table), field_metas_(std::move(field_metas)), exprs_(std::move(expr)){}
  virtual ~UpdateLogicalOperator() = default;

  LogicalOperatorType type() const override { return LogicalOperatorType::UPDATE; }
  Table              *table() const { return table_; }
  std::vector<FieldMeta> field_metas() const { return field_metas_; }
  std::vector<std::unique_ptr<Expression>>&     exprs() {return exprs_; }

private:
  Table *table_;
  std::vector<FieldMeta> field_metas_;
  std::vector<std::unique_ptr<Expression>>     exprs_;
};
