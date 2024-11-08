#include "vector_index_meta.h"

#include <common/log/log.h>
#include <json/value.h>

const static Json::StaticString FIELD_NAME("name");
const static Json::StaticString FIELD_FIELD_NAME("field_name");
const static Json::StaticString FIELD_DISTANCE_TYPE("distance_type");
const static Json::StaticString FIELD_LISTS("lists");
const static Json::StaticString FIELD_PROBES("probes");

RC VectorIndexMeta::init(
    const std::string &name, const FieldMeta &field_meta, DistanceType distance_type, size_t lists, size_t probes)
{
  name_          = name;
  field_meta_    = field_meta;
  distance_type_ = distance_type;
  lists_         = lists;
  probes_        = probes;
  return RC::SUCCESS;
}

const std::string &VectorIndexMeta::name() const { return name_; }

const FieldMeta &VectorIndexMeta::field_meta() const { return field_meta_; }

DistanceType VectorIndexMeta::distance_type() const { return distance_type_; }
size_t       VectorIndexMeta::lists() const { return lists_; }
size_t       VectorIndexMeta::probes() const { return probes_; }

void VectorIndexMeta::to_json(Json::Value &json_value) const
{
  json_value[FIELD_NAME] = name_;
  Json::Value field_meta_json;
  field_meta_.to_json(field_meta_json);
  json_value[FIELD_FIELD_NAME]    = field_meta_json;
  json_value[FIELD_DISTANCE_TYPE] = static_cast<int>(distance_type_);
  json_value[FIELD_LISTS]         = lists_;
  json_value[FIELD_PROBES]        = probes_;
}

RC VectorIndexMeta::from_json(const TableMeta &table, const Json::Value &json_value, VectorIndexMeta &vector_index)
{
  const Json::Value &name_value          = json_value[FIELD_NAME];
  const Json::Value &field_value         = json_value[FIELD_FIELD_NAME];
  const Json::Value &distance_type_value = json_value[FIELD_DISTANCE_TYPE];
  const Json::Value &lists_value         = json_value[FIELD_LISTS];
  const Json::Value &probes_value        = json_value[FIELD_PROBES];
  if (!name_value.isString()) {
    LOG_ERROR("Vector index name is not a string. json value=%s", name_value.toStyledString().c_str());
    return RC::INTERNAL;
  }
  FieldMeta field_meta;
  RC        rc = FieldMeta::from_json(field_value, field_meta);
  if (OB_FAIL(rc)) {
    LOG_ERROR("Failed to deserialize field meta. json value=%s", field_value.toStyledString().c_str());
    return rc;
  }
  if (!distance_type_value.isInt64()) {
    LOG_ERROR("Vector index distance type is not a int64. json value=%s", name_value.toStyledString().c_str());
    return RC::INTERNAL;
  }
  if (!lists_value.isInt64()) {
    LOG_ERROR("Vector index lists is not a int64. json value=%s", name_value.toStyledString().c_str());
    return RC::INTERNAL;
  }
  if (!probes_value.isInt64()) {
    LOG_ERROR("Vector index probes is not a int64. json value=%s", name_value.toStyledString().c_str());
    return RC::INTERNAL;
  }
  return vector_index.init(name_value.asString(),
      field_meta,
      static_cast<DistanceType>(distance_type_value.asInt()),
      lists_value.asUInt64(),
      probes_value.asUInt64());
}

void VectorIndexMeta::desc(ostream &os) const
{
  os << "index name=" << name_ << ", field=" << field_meta_.name()
     << "distance_type=" << static_cast<int>(distance_type_) << "lists=" << lists_ << "probes=" << probes_ << "]";
}
