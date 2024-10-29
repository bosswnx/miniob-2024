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
// Created by Wangyunlai 2023/6/27
//

#pragma once

#include "common/lang/string.h"
#include "common/lang/memory.h"
#include "common/type/attr_type.h"
#include "common/type/data_type.h"

/**
 * @brief 属性的值
 * @ingroup DataType
 * @details 与DataType，就是数据类型，配套完成各种算术运算、比较、类型转换等操作。这里同时记录了数据的值与类型。
 * 当需要对值做运算时，建议使用类似 Value::add 的操作而不是 DataType::add。在进行运算前，应该设置好结果的类型，
 * 比如进行两个INT类型的除法运算时，结果类型应该设置为FLOAT。
 * NULL 很特殊，当一个 Value 的值是 NULL 的时候，不会考虑他是什么类型的。
 */
class Value final
{
public:
  friend class DataType;
  friend class IntegerType;
  friend class FloatType;
  friend class BooleanType;
  friend class CharType;
  friend class DateType;
  friend class VectorType;
  friend class TextType;
  /// 默认构造非空的Value
  Value() = default;

  ~Value() { reset(); }

  Value(AttrType attr_type, char *data, int length = 4) : attr_type_(attr_type) { this->set_data(data, length); }

  explicit Value(int val);
  explicit Value(float val);
  explicit Value(bool val);
  explicit Value(const char *s, int len = 0);

  static Value TextValue(const char *s, int len);

  /// 构造类型未定义的 NULL
  static Value NullValue();

  static Value *from_date(const char *s);
  static Value *from_vector(const char *s);

  Value(const Value &other);
  Value(Value &&other);

  Value &operator=(const Value &other);
  Value &operator=(Value &&other) noexcept;

  void reset();

  // 判断并设置二元运算结果的类型：add, subtract, multiply, divide, max, min
  // 注意：这里没有处理 NULL 的情况，NULL 的情况在每个运算中单独处理，
  // 算数运算中 NULL 参与运算结果为 NULL，max 和 min 中一方为 NULL 结果为另一方
  static RC set_result_type(const Value &left, const Value &right, Value &result)
  {
    switch (left.attr_type()) {
      case AttrType::INTS:
        switch (right.attr_type()) {
          case AttrType::INTS:
          case AttrType::BOOLEANS: result.set_type(AttrType::INTS); break;
          case AttrType::FLOATS: result.set_type(AttrType::FLOATS); break;
          default: return RC::VALUE_TYPE_MISMATCH;
        }
        break;
      case AttrType::FLOATS:
        switch (right.attr_type()) {
          case AttrType::INTS:
          case AttrType::BOOLEANS:
          case AttrType::FLOATS: result.set_type(AttrType::FLOATS); break;
          default: return RC::VALUE_TYPE_MISMATCH;
        }
        break;
      case AttrType::CHARS:
        switch (right.attr_type()) {
          case AttrType::CHARS: result.set_type(AttrType::CHARS); break;
          default: return RC::VALUE_TYPE_MISMATCH;
        }
        break;
      case AttrType::BOOLEANS:
        switch (right.attr_type()) {
          case AttrType::INTS: result.set_type(AttrType::INTS); break;
          case AttrType::FLOATS: result.set_type(AttrType::FLOATS); break;
          default: return RC::VALUE_TYPE_MISMATCH;
        }
        break;
      case AttrType::DATES:
        switch (right.attr_type()) {
          case AttrType::DATES: result.set_type(AttrType::DATES); break;
          default: return RC::VALUE_TYPE_MISMATCH;
        }
        break;
      case AttrType::VECTORS:
        switch (right.attr_type()) {
          case AttrType::VECTORS: result.set_type(AttrType::VECTORS); break;
          default: return RC::VALUE_TYPE_MISMATCH;
        }
        break;
      default: return RC::VALUE_TYPE_MISMATCH;
    }
    return RC::SUCCESS;
  }

  // 与 Value 有关的运算都在这里。
  // 需要先调用 set_result_type 设置结果类型，再调用 DataType 的运算方法

  static RC add(const Value &left, const Value &right, Value &result)
  {
    if (left.is_null() || right.is_null()) {
      result.set_null();
      return RC::SUCCESS;
    }
    RC rc = set_result_type(left, right, result);
    if (rc != RC::SUCCESS) {
      return rc;
    }
    return DataType::type_instance(result.attr_type())->add(left, right, result);
  }

  static RC subtract(const Value &left, const Value &right, Value &result)
  {
    if (left.is_null() || right.is_null()) {
      result.set_null();
      return RC::SUCCESS;
    }
    RC rc = set_result_type(left, right, result);
    if (rc != RC::SUCCESS) {
      return rc;
    }
    return DataType::type_instance(result.attr_type())->subtract(left, right, result);
  }

  static RC multiply(const Value &left, const Value &right, Value &result)
  {
    if (left.is_null() || right.is_null()) {
      result.set_null();
      return RC::SUCCESS;
    }
    RC rc = set_result_type(left, right, result);
    if (rc != RC::SUCCESS) {
      return rc;
    }
    return DataType::type_instance(result.attr_type())->multiply(left, right, result);
  }

  static RC divide(const Value &left, const Value &right, Value &result)
  {
    if (left.is_null() || right.is_null()) {
      result.set_null();
      return RC::SUCCESS;
    }
    RC rc = set_result_type(left, right, result);
    if (rc != RC::SUCCESS) {
      return rc;
    }
    // 除法运算结果类型为 FLOATS
    if (result.attr_type() == AttrType::INTS) {
      result.set_type(AttrType::FLOATS);
    }
    return DataType::type_instance(result.attr_type())->divide(left, right, result);
  }

  static RC negative(const Value &value, Value &result)
  {
    if (value.is_null()) {
      result.set_null();
      return RC::SUCCESS;
    }
    return DataType::type_instance(result.attr_type())->negative(value, result);
  }

  static RC max(const Value &left, const Value &right, Value &result)
  {
    if (left.is_null()) {
      result = right;
      return RC::SUCCESS;
    }
    if (right.is_null()) {
      result = left;
      return RC::SUCCESS;
    }
    RC rc = set_result_type(left, right, result);
    if (rc != RC::SUCCESS) {
      return rc;
    }
    return DataType::type_instance(result.attr_type())->max(left, right, result);
  }

  static RC min(const Value &left, const Value &right, Value &result)
  {
    if (left.is_null()) {
      result = right;
      return RC::SUCCESS;
    }
    if (right.is_null()) {
      result = left;
      return RC::SUCCESS;
    }
    RC rc = set_result_type(left, right, result);
    if (rc != RC::SUCCESS) {
      return rc;
    }
    return DataType::type_instance(result.attr_type())->min(left, right, result);
  }

  static RC cast_to(const Value &value, AttrType to_type, Value &result)
  {
    // NULL 可以转型到任意类型
    // 场景 1：构造出的 NULL 是未定义类型，需要转型到正确类型
    if (value.is_null()) {
      result.set_type(to_type);
      result.set_null();
      return RC::SUCCESS;
    }
    return DataType::type_instance(value.attr_type())->cast_to(value, to_type, result);
  }

  void set_type(AttrType type) { this->attr_type_ = type; }
  void set_data(char *data, int length);
  void set_data(const char *data, int length) { this->set_data(const_cast<char *>(data), length); }
  void set_value(const Value &value);
  void set_boolean(bool val);
  void set_null();

  string to_string() const;

  int compare(const Value &other) const;

  const char *data() const;

  int      length() const { return length_; }

  /// 复制数据时使用
  /// 保证字符串结尾的 \0 也能被复制
  /// 正确处理text 和 null
  int data_length() const
  {
    if (attr_type_ == AttrType::CHARS) {
      return length_ + 1;
    } else if (attr_type_ == AttrType::TEXTS) {
      return offsetof(TextData, TextData::len) + sizeof(TextData::len);  // 只复制前两项成员
    } else {
      return length_;     // null 返回 0
    }
  }

  AttrType           attr_type() const { return attr_type_; }
  [[nodiscard]] bool is_date_valid() const;

public:
  /**
   * 获取对应的值
   * 如果当前的类型与期望获取的类型不符，就会执行转换操作
   */
  int    get_int() const;
  float  get_float() const;
  string get_string() const;
  bool   get_boolean() const;
  vector<float> get_vector() const;
  
  // 这里把下面的 setter 都放到了 public 里面，这样可以直接通过 Value 对象调用这些方法
  void set_int(int val);
  void set_float(float val);
  void set_string(const char *s, int len = 0);
  void set_date(const char *s);  // 从 "YYYY-MM-DD" 格式的日期字符串创建 Value
  void set_date(int val);        // 从 YYYYMMDD 格式的整数创建 Value
  void set_vector(const char *s);
  void set_vector(const vector<float> &vec);
  void set_string_from_other(const Value &other);
  void set_text_from_other(const Value &other);
  void set_text(const char *s, int len, bool give_ownership = false);

  bool is_int() const { return attr_type_ == AttrType::INTS; }
  bool is_float() const { return attr_type_ == AttrType::FLOATS; }
  bool is_boolean() const { return attr_type_ == AttrType::BOOLEANS; }
  bool is_chars() const { return attr_type_ == AttrType::CHARS; }
  bool is_vector() const { return attr_type_ == AttrType::VECTORS; }
  bool is_date() const { return attr_type_ == AttrType::DATES; }
  bool is_null() const { return attr_type_ == AttrType::NULLS; }

private:
  AttrType attr_type_ = AttrType::UNDEFINED;
  int      length_    = 0;  // 对于向量数据，length_ 表示向量的维度

  union Val
  {
    int32_t int_value_;
    float   float_value_;
    bool    bool_value_;
    char   *pointer_value_;
    vector<float> *vector_value_; // 向量数据
    TextData       text_value_;
  } value_ = {.int_value_ = 0};

  /// 是否申请并占有内存, 目前对于 CHARS 和 VECTORS 和 TEXT 类型 own_data_ 为true, 其余类型 own_data_ 为false
  bool own_data_ = false;
};
