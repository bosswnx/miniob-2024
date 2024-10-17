#include "drop_table_executor.h"

#include <event/session_event.h>
#include <event/sql_event.h>
#include <session/session.h>
#include <sql/stmt/drop_table_stmt.h>
#include <storage/db/db.h>

RC DropTableExecutor::execute(SQLStageEvent *sql_event) {
    Db* db =  sql_event->session_event()->session()->get_current_db();
    Stmt* t = sql_event->stmt();
    // DropTableStmt* stmt = (DropTableStmt *)t;
    DropTableStmt* stmt = dynamic_cast<DropTableStmt*>(t);
    ASSERT(stmt != nullptr, "DropTableStmt is nullptr");
    db->drop_table(stmt->table_name().c_str());
    return RC::SUCCESS;
}
