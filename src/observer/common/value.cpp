/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by WangYunlai on 2023/06/28.
//

#include <sstream>
#include <limits>

#include "common/value.h"

#include "common/lang/comparator.h"
#include "common/lang/exception.h"
#include "common/lang/sstream.h"
#include "common/lang/string.h"
#include "common/log/log.h"


Value::Value(int val) { set_int(val); }

Value::Value(float val) { set_float(val); }

Value::Value(bool val) { set_boolean(val); }

Value::Value(const char *s, int len /*= 0*/) { set_string(s, len); }

Value Value::TextValue(const char *s, int len)
{
  Value result;
  result.set_text(s, len);
  return result;
}
Value Value::NullValue()
{
  Value value;
  value.set_null();
  return value;
}
// 从 YYYY-MM-DD 格式的日期字符串创建 Value
// 这个阶段不检查日期的合法性
Value *Value::from_date(const char *s)
{
  auto *val = new Value();
  val->set_date(s);
  return val;
}

// 从向量字符串创建 Value
Value *Value::from_vector(const char *s)
{
  Value *val = new Value();
  val->set_vector(s);
  return val;
}

Value::Value(const Value &other)
{
  this->attr_type_ = other.attr_type_;
  this->length_    = other.length_;
  this->own_data_  = other.own_data_;
  switch (this->attr_type_) {
    case AttrType::CHARS: {
      set_string_from_other(other);
    } break;
    case AttrType::TEXTS: {
      set_text_from_other(other);
    } break;
    default: {
      this->value_ = other.value_;
    } break;
  }
}

Value::Value(Value &&other)
{
  this->attr_type_ = other.attr_type_;
  this->length_    = other.length_;
  this->own_data_  = other.own_data_;
  this->value_     = other.value_;
  other.own_data_  = false;
  other.length_    = 0;
}

Value &Value::operator=(const Value &other)
{
  if (this == &other) {
    return *this;
  }
  reset();
  this->attr_type_ = other.attr_type_;
  this->length_    = other.length_;
  this->own_data_  = other.own_data_;
  switch (this->attr_type_) {
    case AttrType::CHARS: {
      set_string_from_other(other);
    } break;
    case AttrType::TEXTS: {
      set_text_from_other(other);
    } break;
    default: {
      this->value_ = other.value_;
    } break;
  }
  return *this;
}

Value &Value::operator=(Value &&other) noexcept
{
  if (this == &other) {
    return *this;
  }
  reset();
  this->attr_type_ = other.attr_type_;
  this->length_    = other.length_;
  this->own_data_  = other.own_data_;
  this->value_     = other.value_;
  other.own_data_  = false;
  other.length_    = 0;
  return *this;
}

void Value::reset()
{
  switch (attr_type_) {
    case AttrType::CHARS:
      if (own_data_ && value_.pointer_value_ != nullptr) {
        delete[] value_.pointer_value_;
        value_.pointer_value_ = nullptr;
      }
      break;
    case AttrType::TEXTS: {
      if (own_data_ && value_.text_value_.str != nullptr) {
        delete[] value_.text_value_.str;
        value_.text_value_.str = nullptr;
      }
    }
    default: break;
  }

  attr_type_ = AttrType::UNDEFINED;
  length_    = 0;
  own_data_  = false;
}

// set data 前必须要先设置 type
void Value::set_data(char *data, int length)
{
  switch (attr_type_) {
    case AttrType::CHARS: {
      set_string(data, length);
    } break;
    case AttrType::INTS: {
      // 避免未对齐的数据访问
      memcpy(&value_.int_value_, data, length);
      length_ = length;
    } break;
    case AttrType::FLOATS: {
      memcpy(&value_.float_value_, data, length);
      length_ = length;
    } break;
    case AttrType::BOOLEANS: {
      memcpy(&value_.bool_value_, data, length);
      length_ = length;
    } break;
    case AttrType::DATES: {
      memcpy(&value_.int_value_, data, length);
      length_ = length;
    } break;
    case AttrType::VECTORS: {
      int           offset = 0;
      vector<float> vec;
      while (offset < length * sizeof(float)) {
        float f;
        memcpy(&f, data + offset, sizeof(float));
        // 如果 f 是 nan，说明数据已经读完
        if (f != f) {
          break;
        }
        vec.push_back(f);
        offset += sizeof(float);
      }
      value_.vector_value_ = new vector<float>(vec);
      length_ = value_.vector_value_->size(); // Value 的 length_ 是向量的维度
    } break;
    case AttrType::TEXTS: {
      memcpy(&value_.text_value_, data, length);
      length_ = length;
    } break;
    default: {
      ASSERT(false, "unknown data type: %d", attr_type_);
    } break;
  }
}

void Value::set_int(int val)
{
  reset();
  attr_type_        = AttrType::INTS;
  value_.int_value_ = val;
  length_           = sizeof(val);
}

void Value::set_float(float val)
{
  reset();
  attr_type_          = AttrType::FLOATS;
  value_.float_value_ = val;
  length_             = sizeof(val);
}
void Value::set_boolean(bool val)
{
  reset();
  attr_type_         = AttrType::BOOLEANS;
  value_.bool_value_ = val;
  length_            = sizeof(val);
}

void Value::set_string(const char *s, int len /*= 0*/)
{
  reset();
  attr_type_ = AttrType::CHARS;
  if (s == nullptr) {
    value_.pointer_value_ = nullptr;
    length_               = 0;
  } else {
    own_data_ = true;
    if (len > 0) {
      len = strnlen(s, len);
    } else {
      len = strlen(s);
    }
    value_.pointer_value_ = new char[len + 1];
    length_               = len;
    memcpy(value_.pointer_value_, s, len);
    value_.pointer_value_[len] = '\0';
  }
}

// 将输入的日期字符串转换为 int 类型存储
void Value::set_date(const char *s)
{
  reset();
  attr_type_        = AttrType::DATES;
  string date       = s;
  string            dates;  // 存储分割好的日期字符串
  std::stringstream ss(date);
  std::string       part;
  while (std::getline(ss, part, '-')) {
    if (part.length() == 1) {
      dates += "0" + part;
    } else {
      dates += part;
    }
  }
  value_.int_value_ = atoi(dates.c_str());
  length_           = sizeof(value_.int_value_);
}

void Value::set_date(int val)
{
  reset();
  attr_type_        = AttrType::DATES;
  value_.int_value_ = val;
  length_           = sizeof(val);
}

void Value::set_vector(const char *s)
{
  reset();
  attr_type_     = AttrType::VECTORS;
  string vector_ = s;
  vector_        = vector_.substr(1, vector_.size() - 2);  // 去掉中括号
  vector<float>      vec;
  std::istringstream iss(vector_);
  string             token;
  while (std::getline(iss, token, ',')) {
    vec.push_back(std::stof(token));
  }
  value_.vector_value_ = new vector<float>(vec);
  length_              = value_.vector_value_->size();
}

void Value::set_vector(const vector<float> &vec)
{
  reset();
  attr_type_           = AttrType::VECTORS;
  value_.vector_value_ = new vector<float>(vec);
  length_              = sizeof(value_.vector_value_);
}

void Value::set_value(const Value &value)
{
  reset();
  if (value.is_null()) {
    set_null();
    return;
  }
  switch (value.attr_type_) {
    case AttrType::INTS: {
      set_int(value.get_int());
    } break;
    case AttrType::FLOATS: {
      set_float(value.get_float());
    } break;
    case AttrType::CHARS: {
      set_string(value.get_string().c_str());
    } break;
    case AttrType::BOOLEANS: {
      set_boolean(value.get_boolean());
    } break;
    case AttrType::DATES: {
      set_date(value.get_int());
    } break;
    case AttrType::VECTORS: {
      set_vector(value.get_vector());
    } break;
    case AttrType::TEXTS: {
      set_text(value.value_.text_value_.str, value.value_.text_value_.len);
    } break;
    default: {
      ASSERT(false, "got an invalid value type");
    } break;
  }
}

void Value::set_string_from_other(const Value &other)
{
  ASSERT(attr_type_ == AttrType::CHARS, "attr type is not CHARS");
  if (own_data_ && other.value_.pointer_value_ != nullptr && length_ != 0) {
    this->value_.pointer_value_ = new char[this->length_ + 1];
    memcpy(this->value_.pointer_value_, other.value_.pointer_value_, this->length_);
    this->value_.pointer_value_[this->length_] = '\0';
  }
}

void Value::set_text_from_other(const Value &other)
{
  ASSERT(attr_type_ == AttrType::TEXTS, "attr type is not TEXTS");
  if (own_data_ && other.value_.text_value_.str != nullptr && other.length_ != 0) {
    this->length_ = other.length_;
    auto buffer   = new char[this->length_ + 1];
    memcpy(buffer, other.value_.text_value_.str, this->length_);
    buffer[this->length_]        = '\0';
    this->value_.text_value_.str = buffer;
    this->value_.text_value_.len = this->length_;
  } else {
    ASSERT(false, "own_data == false not handled");
  }
}

void Value::set_text(const char *s, int len, bool give_ownership)
{
  attr_type_ = AttrType::TEXTS;
  if (s == nullptr) {
    length_ = 0;
  } else {
    length_                = len;
    value_.text_value_.len = len;
    own_data_              = true;
    if (!give_ownership) {
      auto buffer = new char[len + 1];
      memcpy(buffer, s, len);
      buffer[len]            = '\0';
      value_.text_value_.str = buffer;
    } else {
      value_.text_value_.str = s;
    }
  }
}
// 设置为 NULL 值
void Value::set_null()
{
  reset();
  attr_type_ = AttrType::NULLS;
}

const char *Value::data() const
{
  switch (attr_type_) {
    case AttrType::CHARS: {
      return value_.pointer_value_;
    } break;
    case AttrType::VECTORS: {
      char *data   = new char[sizeof(float) * length_ + sizeof(float)];
      int   offset = 0;
      for (auto &f : *value_.vector_value_) {
        memcpy(data + offset, &f, sizeof(float));
        offset += sizeof(float);
      }
      // 用 nan 标记数据结束
      float nan = std::numeric_limits<float>::quiet_NaN();
      memcpy(data + offset, &nan, sizeof(float));
      return data;
    }
    case AttrType::TEXTS: {
      return reinterpret_cast<const char *>(&value_.text_value_);
    }
    default: {
      return (const char *)&value_;
    } break;
  }
}

string Value::to_string() const
{
  string res;
  if (is_null()) {
    return "NULL";
  }
  RC rc = DataType::type_instance(this->attr_type_)->to_string(*this, res);
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to convert value to string. type=%s", attr_type_to_string(this->attr_type_));
    return "";
  }
  return res;
}

int Value::compare(const Value &other) const
{
  if (is_null() || other.is_null()) {
    return INT32_MAX;  // 空值参与比较，返回 false
  }
  return DataType::type_instance(this->attr_type_)->compare(*this, other);
}

int Value::get_int() const
{
  switch (attr_type_) {
    case AttrType::CHARS: {
      try {
        return (int)(std::stol(value_.pointer_value_));
      } catch (exception const &ex) {
        LOG_TRACE("failed to convert string to number. s=%s, ex=%s", value_.pointer_value_, ex.what());
        return 0;
      }
    }
    case AttrType::INTS: {
      return value_.int_value_;
    }
    case AttrType::FLOATS: {
      return (int)(value_.float_value_);
    }
    case AttrType::BOOLEANS: {
      return (int)(value_.bool_value_);
    }
    case AttrType::DATES: {
      return value_.int_value_;
    }
    default: {
      LOG_WARN("unknown data type. type=%d", attr_type_);
      return 0;
    }
  }
  return 0;
}

float Value::get_float() const
{
  switch (attr_type_) {
    case AttrType::CHARS: {
      try {
        return std::stof(value_.pointer_value_);
      } catch (exception const &ex) {
        LOG_TRACE("failed to convert string to float. s=%s, ex=%s", value_.pointer_value_, ex.what());
        return 0.0;
      }
    } break;
    case AttrType::INTS: {
      return float(value_.int_value_);
    } break;
    case AttrType::FLOATS: {
      return value_.float_value_;
    } break;
    case AttrType::BOOLEANS: {
      return float(value_.bool_value_);
    } break;
    default: {
      LOG_WARN("unknown data type. type=%d", attr_type_);
      return 0;
    }
  }
  return 0;
}

string Value::get_string() const { return this->to_string(); }

vector<float> Value::get_vector() const
{
  switch (attr_type_) {
    case AttrType::VECTORS: {
      return *value_.vector_value_;
    } break;
    default: {
      LOG_WARN("unknown data type. type=%d", attr_type_);
      return vector<float>();
    }
  }
}

bool Value::get_boolean() const
{
  switch (attr_type_) {
    case AttrType::TEXTS:
    case AttrType::CHARS: {
      const char *str;
      if (attr_type_ == AttrType::TEXTS) {
        str = value_.text_value_.str;
      } else {
        str = value_.pointer_value_;
      }
      try {
        float val = std::stof(str);
        if (val >= EPSILON || val <= -EPSILON) {
          return true;
        }

        int int_val = std::stol(str);
        if (int_val != 0) {
          return true;
        }

        return str != nullptr;
      } catch (exception const &ex) {
        LOG_TRACE("failed to convert string to float or integer. s=%s, ex=%s", str  , ex.what());
        return str != nullptr;
      }
    } break;
    case AttrType::INTS: {
      return value_.int_value_ != 0;
    } break;
    case AttrType::FLOATS: {
      float val = value_.float_value_;
      return val >= EPSILON || val <= -EPSILON;
    } break;
    case AttrType::BOOLEANS: {
      return value_.bool_value_;
    } break;
    default: {
      LOG_WARN("unknown data type. type=%d", attr_type_);
      return false;
    }
  }
  return false;
}

// 不会判断是否是 date 类型，需要调用者提前自己判断
bool Value::is_date_valid() const
{
  if (is_null()) {
    return true;
  }
  int date  = get_int();
  int year  = date / 10000;
  int month = (date % 10000) / 100;
  int day   = date % 100;
  if (year < 1900 || year > 2100) {
    return false;
  }
  if (month < 1 || month > 12) {
    return false;
  }
  if (day < 1 || day > 31) {
    return false;
  }
  // 判断闰年
  if (month == 2) {
    bool is_leap_year = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    if (is_leap_year) {
      return day <= 29;
    } else {
      return day <= 28;
    }
  }
  if (month == 4 || month == 6 || month == 9 || month == 11) {
    return day <= 30;
  }
  return true;
}
