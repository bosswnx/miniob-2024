#include "update_physical_opeator.h"

RC UpdatePhysicalOperator::open(Trx *trx)
{
  auto &child = children_[0];
  RC    rc    = child->open(trx);
  if (OB_FAIL(rc)) {
    LOG_WARN("child operator open failed: %s", strrc(rc));
    return rc;
  }
  while (OB_SUCC(rc = child->next())) {
    Tuple *tuple_ = child->current_tuple(); // 获得当前正在更新的 tuple
    auto   tuple  = dynamic_cast<RowTuple *>(tuple_);
    ASSERT(tuple != nullptr, "tuple cannot cast to RowTuple here!");
    rc = table_->visit_record(tuple->record().rid(), [this, tuple](Record &record) {
      table_->index_cache_delete_entry(record, field_metas_);
      std::vector<Value> cells_to_update; // 先存，防止有一个 field 更新异常导致部分写入。
      for (size_t i = 0; i < exprs_.size(); i++) {
        Value cell;
        RC    rc = exprs_[i]->get_value(*tuple, cell);

        // 子查询返回空值的情况
        if (rc == RC::RECORD_EOF && field_metas_[i].nullable()) {
          cell.set_null();
        } else if (OB_FAIL(rc)) {
          LOG_WARN("cannot get value from expression: %s", strrc(rc));
        }

        if (cell.is_null() && !field_metas_[i].nullable()) {
          LOG_WARN("field %s is not nullable, but the value is null", field_metas_[i].name());
          return RC::INVALID_ARGUMENT;
        }

        // we get the value again to check if the subquery is legal
        if (exprs_[i]->type() == ExprType::SUB_QUERY) {
          Value test_cell_;
          RC rc_ = exprs_[i]->get_value(*tuple, test_cell_);
          if (rc_ != RC::RECORD_EOF) {
            LOG_WARN("update: subquery should return only one value");
            return RC::INVALID_ARGUMENT;
          }
        }
        
        if (cell.attr_type() != field_metas_[i].type()) {
          Value to_value;
          rc = Value::cast_to(cell, field_metas_[i].type(), to_value);
          if (OB_FAIL(rc)) {
            LOG_WARN("cannot cast from %s to %s", attr_type_to_string(cell.attr_type()), attr_type_to_string(field_metas_[i].type()));
            return RC::INVALID_ARGUMENT;
          }
          cell = to_value;
        }
        // 检查向量长度是否超过限制
        if (cell.attr_type() == AttrType::VECTORS &&
            static_cast<size_t>(cell.length()) > field_metas_[i].vector_dim() * sizeof(float)) {
          LOG_WARN("vector length exceeds limit: %d > %d", cell.data_length()/4, field_metas_[i].vector_dim());
          return RC::INVALID_ARGUMENT;
        }
        // tuple->set_cell_at(field_metas_[i].field_id(), cell, record.data());
        cells_to_update.push_back(cell);
      }
      for (size_t i = 0; i<cells_to_update.size(); ++i) {
        tuple->set_cell_at(field_metas_[i].field_id(), cells_to_update[i], record.data());
      }
      cells_to_update.clear();

      table_->index_cache_insert_entry(record, field_metas_);

      return RC::SUCCESS;
    });
    if (rc != RC::SUCCESS) {
      return rc;
    }
  }

  // 下层算子可能使用索引获取记录，需要推迟索引的更新，避免利用索引遍历记录的同时更新索引
  child->close();
  rc = table_->index_flush_cached_entries();
  if (OB_FAIL(rc)) {
    LOG_WARN("update index failed: %s", strrc(rc));
    return rc;
  }
  return RC::SUCCESS;
}

RC UpdatePhysicalOperator::close() { return RC::SUCCESS; }

RC UpdatePhysicalOperator::next() { return RC::RECORD_EOF; }
