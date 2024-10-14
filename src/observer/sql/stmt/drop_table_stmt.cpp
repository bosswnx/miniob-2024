#include "sql/stmt/drop_table_stmt.h"
#include "storage/db/db.h"

RC DropTableStmt::create(const Db *db, const DropTableSqlNode &sql_node, Stmt *&stmt) {
    if (db->find_table(sql_node.relation_name.c_str()) == nullptr) {
        LOG_ERROR("table %s doesn't exist!", sql_node.relation_name.c_str());
        return RC::SCHEMA_TABLE_NOT_EXIST;
    }
    stmt = new DropTableStmt(sql_node.relation_name);
    return RC::SUCCESS;
}
