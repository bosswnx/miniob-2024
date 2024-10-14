#pragma once

#include "sql/stmt/stmt.h"

class DropTableStmt :public Stmt {
public:
    explicit DropTableStmt(std::string table_name_): table_name_(std::move(table_name_)) {
    }

    ~DropTableStmt() override = default;

    StmtType type() const override {
        return StmtType::DROP_TABLE;
    }

    [[nodiscard]] const std::string &table_name() const {
        return table_name_;
    }

    static RC create(const Db *db, const DropTableSqlNode &sql_node, Stmt *&stmt);

private:
    std::string table_name_;
};
