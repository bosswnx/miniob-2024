#pragma once

#include <string>

#include "sql/stmt/stmt.h"
#include "storage/index/vector_index_meta.h"

struct CreateIndexSqlNode;
class Table;
class FieldMeta;

class CreateVectorIndexStmt : public Stmt
{
public:
  CreateVectorIndexStmt(Table *table, const FieldMeta *field_metas, const std::string &index_name,
      DistanceType distance_type, size_t lists, size_t probes)
      : table_(table),
        field_meta_(field_metas),
        index_name_(index_name),
        distance_type_(distance_type),
        lists_(lists),
        probes_(probes)
  {}

  ~CreateVectorIndexStmt() override = default;

  StmtType type() const override { return StmtType::CREATE_VECTOR_INDEX; }

  Table             *table() const { return table_; }
  const FieldMeta   *field_meta() const { return field_meta_; }
  const std::string &index_name() const { return index_name_; }
  DistanceType       distance_type() const { return distance_type_; }
  size_t             lists() const { return lists_; }
  size_t             probes() const { return probes_; }

public:
  static RC create(Db *db, const CreateVectorIndexSqlNode &create_vector_index, Stmt *&stmt);

private:
  Table           *table_ = nullptr;
  const FieldMeta *field_meta_;
  std::string      index_name_;
  DistanceType     distance_type_;
  size_t           lists_;
  size_t           probes_;
};
