#pragma once

#include "common/type/data_type.h"

class TextType : public DataType
{
public:
  TextType() : DataType(AttrType::TEXTS){};

  ~TextType() override = default;

  int compare(const Value &left, const Value &right) const override;

  RC cast_to(const Value &val, AttrType type, Value &result) const override;

  RC set_value_from_str(Value &val, const string &data) const override;

  int cast_cost(AttrType type) override;

  RC to_string(const Value &val, string &result) const override;
};