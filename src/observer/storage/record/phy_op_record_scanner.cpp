#include "storage/trx/trx.h"
#include "storage/record/phy_op_record_scanner.h"


RC RecordPhysicalOperatorScanner::open_oper(Trx *trx) {
  RC rc = RC::SUCCESS;
  if (oper_ == nullptr) {
    LOG_PANIC("RecordPhysicalOperatorScanner: physical operator is null");
    return RC::INVALID_ARGUMENT;
  }
  rc = oper_->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("failed to open physical operator. rc=%d:%s", rc, strrc(rc));
    return rc;
  }
  return rc;
}

RC RecordPhysicalOperatorScanner::close_scan() {
  RC rc = RC::SUCCESS;
  if (oper_ != nullptr) {
    rc = oper_->close();
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to close physical operator. rc=%d:%s", rc, strrc(rc));
      return rc;
    }
    oper_ = nullptr;
  }
  return rc;
}

RC RecordPhysicalOperatorScanner::next(Record &record) {
  LOG_PANIC("RecordPhysicalOperatorScanner: next is not implemented, should use next_tuple() instead");
  return RC::UNIMPLEMENTED;
}

RC RecordPhysicalOperatorScanner::next_tuple() {
  RC rc = RC::SUCCESS;
  if (oper_ == nullptr) {
    LOG_PANIC("RecordPhysicalOperatorScanner: physical operator is null");
  }
  rc = oper_->next();
  if (rc != RC::SUCCESS) {
    if (rc != RC::RECORD_EOF) {
      LOG_WARN("failed to get next record from physical operator. rc=%d:%s", rc, strrc(rc));
    }
    return rc;
  }
  tuple = oper_->current_tuple();
  return rc;
}
