#include "create_vector_index_executor.h"
#include "common/log/log.h"
#include "event/session_event.h"
#include "event/sql_event.h"
#include "session/session.h"
#include "sql/stmt/create_index_stmt.h"
#include "storage/table/table.h"

#include <sql/stmt/create_vector_index_stmt.h>

RC CreateVectorIndexExecutor::execute(SQLStageEvent *sql_event)
{
  Stmt    *stmt    = sql_event->stmt();
  Session *session = sql_event->session_event()->session();
  ASSERT(stmt->type() == StmtType::CREATE_VECTOR_INDEX,
      "create vector index executor can not run this command: %d",
      static_cast<int>(stmt->type()));

  auto create_index_stmt = static_cast<CreateVectorIndexStmt *>(stmt);

  Trx   *trx   = session->current_trx();
  Table *table = create_index_stmt->table();
  return table->create_vector_index(trx,
      create_index_stmt->field_meta(),
      create_index_stmt->index_name().c_str(),
      create_index_stmt->distance_type(),
      create_index_stmt->lists(),
      create_index_stmt->probes());
}
