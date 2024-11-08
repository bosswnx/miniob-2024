#pragma once
#include "common/rc.h"
#include "common/lang/string.h"
#include "storage/field/field_meta.h"
#include "storage/table/table_meta.h"

enum class DistanceType
{
  INNER_PRODUCT,
  L2_DISTANCE,
  COSINE_DISTANCE
};
namespace Json {
class Value;
}  // namespace Json

class VectorIndexMeta
{
public:
  VectorIndexMeta() = default;

  RC init(
      const std::string &name, const FieldMeta &field_meta, DistanceType distance_type, size_t lists, size_t probes);

  [[nodiscard]] const std::string &name() const;
  [[nodiscard]] const FieldMeta   &field_meta() const;
  [[nodiscard]] DistanceType       distance_type() const;
  [[nodiscard]] size_t             lists() const;
  [[nodiscard]] size_t             probes() const;
  void                             to_json(Json::Value &json_value) const;
  static RC from_json(const TableMeta &table, const Json::Value &json_value, VectorIndexMeta &vector_index);
  void      desc(ostream &os) const;

private:
  std::string  name_;
  FieldMeta    field_meta_;
  DistanceType distance_type_;
  size_t       lists_;
  size_t       probes_;
};