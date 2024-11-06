#include "storage/record/record_manager.h"
#include "sql/operator/physical_operator.h"

class PhysicalOperator;

class RecordPhysicalOperatorScanner : public RecordFileScanner
{
public:
RecordPhysicalOperatorScanner() = default;
  ~RecordPhysicalOperatorScanner() = default;
  RC open_oper(Trx *trx);
  RC close_scan();
  RC next(Record &record);
  RC next_tuple();

  void set_oper(unique_ptr<PhysicalOperator> oper) { oper_ = std::move(oper); }

  Tuple *current_tuple() { return tuple; }
private:
  unique_ptr<PhysicalOperator> oper_;
  Tuple *tuple;
};
