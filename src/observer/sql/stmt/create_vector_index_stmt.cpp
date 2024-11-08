#include "create_vector_index_stmt.h"

#include "storage/db/db.h"
#include "storage/table/table.h"
#include "storage/table/table_meta.h"
#include "common/lang/string.h"

RC CreateVectorIndexStmt::create(Db *db, const CreateVectorIndexSqlNode &create_vector_index, Stmt *&stmt)
{
  Table *table = db->find_table(create_vector_index.relation_name.c_str());
  if (table == nullptr) {
    LOG_WARN("table %s not found", create_vector_index.relation_name.c_str());
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }
  const TableMeta &table_meta = table->table_meta();
  const FieldMeta *field_meta = table_meta.field(create_vector_index.attribute_names.c_str());
  if (field_meta == nullptr) {
    LOG_WARN("field %s not found", create_vector_index.attribute_names.c_str());
    return RC::SCHEMA_FIELD_MISSING;
  }
  std::string  index_name   = create_vector_index.index_name;
  VectorIndex *vector_index = table->find_vector_index(index_name.c_str());
  if (vector_index != nullptr) {
    LOG_WARN("vector index %s exists", index_name);
    return RC::SCHEMA_INDEX_NAME_REPEAT;
  }

  DistanceType distance_type = DistanceType::L2_DISTANCE;
  size_t       lists         = -1;
  size_t       probes        = -1;
  for (int i = 0; i < 4; i++) {
    auto        pair  = create_vector_index.params[i];
    std::string key   = pair.first;
    std::string value = pair.second;
    common::str_to_lower(key);
    common::str_to_lower(value);
    if (key == "distance") {
      if (value == "l2_distance") {
        distance_type = DistanceType::L2_DISTANCE;
      } else if (value == "inner_product") {
        distance_type = DistanceType::INNER_PRODUCT;
      } else if (value == "cosine_distance") {
        distance_type = DistanceType::COSINE_DISTANCE;
      } else {
        LOG_WARN("unsupported vector index parameter %s", pair.second.c_str());
        return RC::UNSUPPORTED;
      }
    } else if (key == "type") {
      if (value == "ivfflat") {
        // nop
      } else {
        LOG_WARN("unsupported vector index parameter %s", pair.second.c_str());
        return RC::UNSUPPORTED;
      }
    } else if (key == "lists") {
      lists = std::stol(value);
    } else if (key == "probes") {
      probes = std::stol(value);
    } else {
      LOG_WARN("unsupported vector index parameter %s", pair.first.c_str());
      return RC::UNSUPPORTED;
    }
  }
  stmt = new CreateVectorIndexStmt(table, field_meta, index_name, distance_type, lists, probes);
  return RC::SUCCESS;
}
