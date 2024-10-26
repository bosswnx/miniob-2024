#pragma once

#include "sql/operator/physical_operator.h"

class Trx;
class DeleteStmt;

class UpdatePhysicalOperator : public PhysicalOperator
{
public:
  UpdatePhysicalOperator(
      Table *table, std::vector<FieldMeta> field_metas, std::vector<std::unique_ptr<Expression>> exprs)
      : table_(table), field_metas_(std::move(field_metas)), exprs_(std::move(exprs))
  {}

  ~UpdatePhysicalOperator() override = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::UPDATE; }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  Tuple *current_tuple() override { return nullptr; }

private:
  Table                                   *table_ = nullptr;
  std::vector<FieldMeta>                   field_metas_;
  std::vector<std::unique_ptr<Expression>> exprs_;
};
