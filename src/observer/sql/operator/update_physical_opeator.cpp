#include "update_physical_opeator.h"

RC UpdatePhysicalOperator::open(Trx *trx)
{
  auto &child = children_[0];
  RC    rc    = child->open(trx);
  if (OB_FAIL(rc)) {
    LOG_WARN("child operator open failed: %s", strrc(rc));
    return rc;
  }
  std::vector<std::function<RC()>> updateIndexTasks;
  while (OB_SUCC(rc = child->next())) {
    Tuple *tuple_ = child->current_tuple();
    auto   tuple  = dynamic_cast<RowTuple *>(tuple_);
    ASSERT(tuple != nullptr, "tuple cannot cast to RowTuple here!");
    table_->visit_record(tuple->record().rid(), [this, tuple, trx, &updateIndexTasks](Record &record) {
      for (size_t i = 0; i < exprs_.size(); i++) {
        Value cell;
        RC    rc = exprs_[i]->get_value(*tuple, cell);
        if (OB_FAIL(rc)) {
          LOG_WARN("cannot get value from expression: %s", strrc(rc));
        }
        tuple->set_cell_at(field_metas_[i].field_id(), cell, record.data());
      }
      updateIndexTasks.push_back([this, record] {
        // 复制 record 对象，避免 use-after-free
        return table_->update_index(record, field_metas_);
      });
      return true;
    });
  }

  // 下层算子可能使用索引获取记录，需要推迟索引的更新，避免利用索引遍历记录的同时更新索引
  child->close();
  for (auto task : updateIndexTasks) {
    rc = task();
    if (OB_FAIL(rc)) {
      LOG_WARN("update index failed: %s", strrc(rc));
      return rc;
    }
  }
  return RC::SUCCESS;
}

RC UpdatePhysicalOperator::close() { return RC::SUCCESS; }

RC UpdatePhysicalOperator::next() { return RC::RECORD_EOF; }
