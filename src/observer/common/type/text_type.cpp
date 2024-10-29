#include "text_type.h"

#include "common/value.h"
#include "common/lang/comparator.h"
#include "common/log/log.h"

int TextType::compare(const Value &left, const Value &right) const
{
  ASSERT(left.attr_type() == AttrType::TEXTS && (right.attr_type() == AttrType::CHARS || right.attr_type() == AttrType::TEXTS), "invalid type");
  if (right.attr_type() == AttrType::CHARS) {
    return common::compare_string(
        left.value_.text_value_.str, left.value_.text_value_.len, right.value_.pointer_value_, left.length());
  }
  return common::compare_string(left.value_.text_value_.str,
      left.value_.text_value_.len,
      right.value_.text_value_.str,
      right.value_.text_value_.len);
}

RC TextType::cast_to(const Value &val, AttrType type, Value &result) const
{
  switch (val.attr_type()) {
    case AttrType::CHARS: {
      result.set_string(val.value_.text_value_.str, val.value_.text_value_.len);
      return RC::SUCCESS;
      default: return RC::UNIMPLEMENTED;
    }
  }
}

RC TextType::set_value_from_str(Value &val, const string &data) const
{
  val.set_text(data.c_str(), data.length());
  return RC::SUCCESS;
}

int TextType::cast_cost(AttrType type)
{
  if (type == AttrType::CHARS) {
    return 0;
  }
  return INT32_MAX;
}

RC TextType::to_string(const Value &val, string &result) const
{
  stringstream ss;
  ss << val.value_.text_value_.str;
  result = ss.str();
  return RC::SUCCESS;
}