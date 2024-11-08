#include "vector_index_scan_physical_operator.h"

VectorIndexScanPhysicalOperator::VectorIndexScanPhysicalOperator(
    Table *table, VectorIndex *vector_index, const Value &query_value, int limit)
    : table_(table), vector_index_(vector_index), limit_(limit)
{
  ASSERT(query_value.attr_type() == AttrType::VECTORS, "non vector value not supported");
  const auto vector = query_value.get_vector();
  query_vector_.reserve(vector.dim);
  for (size_t i = 0; i < vector.dim; i++) {
    query_vector_.push_back(vector.vector[i]);
  }
}

RC VectorIndexScanPhysicalOperator::open(Trx *trx)
{
  std::vector<float> distance;
  vector_index_->query(query_vector_.data(), limit_, result, distance);
  result_iterator_ = result.begin();
  record_handler_  = table_->record_handler();
  if (nullptr == record_handler_) {
    LOG_WARN("invalid record handler");
    return RC::INTERNAL;
  }
  tuple_.set_schema(table_, table_->table_meta().field_metas());
  trx_ = trx;
  return RC::SUCCESS;
}

RC VectorIndexScanPhysicalOperator::next()
{
  if (result_iterator_ == result.end()) {
    return RC::RECORD_EOF;
  }
  if (record_handler_ == nullptr) {
    LOG_WARN("internal error");
    return RC::INTERNAL;
  }
  record_handler_->get_record(*result_iterator_, current_record_);
  ++result_iterator_;
  return RC::SUCCESS;
}

RC VectorIndexScanPhysicalOperator::close()
{
  return RC::SUCCESS;
}

Tuple *VectorIndexScanPhysicalOperator::current_tuple()
{
  tuple_.set_record(&current_record_);
  return &tuple_;
}

std::string VectorIndexScanPhysicalOperator::param() const
{
  return vector_index_->meta().name() + " ON " + table_->name();
}
