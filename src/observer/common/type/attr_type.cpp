/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */


#include "common/lang/string.h"
#include "common/type/attr_type.h"

const char *ATTR_TYPE_NAME[] = {
    "undefined", "chars", "ints", "floats", "booleans", "dates", "vectors", "nulls", "texts"};

// 将属性类型转换为字符串
const char *attr_type_to_string(AttrType type)
{
  if (type >= AttrType::UNDEFINED && type < AttrType::MAXTYPE) {
    return ATTR_TYPE_NAME[static_cast<int>(type)];
  }
  return "unknown";
}

AttrType attr_type_from_string(const char *s)
{
  for (unsigned int i = 0; i < sizeof(ATTR_TYPE_NAME) / sizeof(ATTR_TYPE_NAME[0]); i++) {
    if (0 == strcasecmp(ATTR_TYPE_NAME[i], s)) {
      return (AttrType)i;
    }
  }
  return AttrType::UNDEFINED;
}

size_t attr_type_size(AttrType type)
{
  switch (type) {
    case AttrType::CHARS: return sizeof(char);
    case AttrType::DATES:
    case AttrType::INTS: return sizeof(int);
    case AttrType::VECTORS:
    case AttrType::FLOATS: return sizeof(float);
    case AttrType::TEXTS: return offsetof(TextData, TextData::len) + sizeof(TextData::len);
    default: return -1;
  }
}