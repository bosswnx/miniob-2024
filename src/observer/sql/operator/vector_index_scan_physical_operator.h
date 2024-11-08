#pragma once

#include "sql/expr/tuple.h"
#include "sql/operator/physical_operator.h"
#include "storage/record/record_manager.h"

/**
 * @brief 索引扫描物理算子
 * @ingroup PhysicalOperator
 */
class VectorIndexScanPhysicalOperator : public PhysicalOperator
{
public:
  VectorIndexScanPhysicalOperator(Table *table, VectorIndex *vector_index, const Value &query_value, int limit);

  virtual ~VectorIndexScanPhysicalOperator() = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::VECTOR_INDEX_SCAN; }

  std::string param() const override;

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  Tuple *current_tuple() override;

private:
  Trx                       *trx_            = nullptr;
  Table                     *table_          = nullptr;
  VectorIndex               *vector_index_   = nullptr;
  RecordFileHandler         *record_handler_ = nullptr;
  Record                     current_record_;
  RowTuple                   tuple_;
  std::vector<float>         query_vector_;
  std::vector<RID>           result;
  std::vector<RID>::iterator result_iterator_;
  int                        limit_;
};
