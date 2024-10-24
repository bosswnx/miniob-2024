#include "update_physical_opeator.h"

RC UpdatePhysicalOperator::open(Trx *trx)
{
  auto& child = children_[0];
  RC rc = child->open(trx);
  if (OB_FAIL(rc)) {
    LOG_WARN("child operator open failed: %s", strrc(rc));
    return rc;
  }
  while (OB_SUCC(rc = child->next())) {
    Tuple* tuple_ = child->current_tuple();
    auto tuple = dynamic_cast<RowTuple*>(tuple_);
    ASSERT(tuple != nullptr, "tuple cannot cast to RowTuple here!");
    for (size_t i=0; i<exprs_.size(); i++) {
      Value cell;
      rc = exprs_[i]->get_value(*tuple, cell);
      if (OB_FAIL(rc)) {
        LOG_WARN("cannot get value from expression: %s", strrc(rc));
      }
      trx->visit_record(table_, tuple->record(), ReadWriteMode::READ_WRITE);
      tuple->set_cell_at(field_metas_[i].field_id(), cell);
    }
  }
  return RC::SUCCESS;
}

RC UpdatePhysicalOperator::close(){
  children_[0]->close();
  return RC::SUCCESS;
}

RC UpdatePhysicalOperator::next(){
  return RC::RECORD_EOF;
}

