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
// Created by Meiyi & Wangyunlai on 2021/5/11.
//

#pragma once

#include <stddef.h>
#include <vector>
#include <cstring>

#include "common/rc.h"
#include "storage/field/field_meta.h"
#include "storage/index/index_meta.h"
#include "storage/record/record_manager.h"

class IndexScanner;
class IndexUserKey;

const int KEY_NULL_BYTE = 4;

/**
 * @brief 索引
 * @defgroup Index
 * @details 索引可能会有很多种实现，比如B+树、哈希表等，这里定义了一个基类，用于描述索引的基本操作。
 */

/**
 * @brief 索引基类
 * @ingroup Index
 */
class Index
{
public:
  Index()          = default;
  virtual ~Index() = default;

  const IndexMeta &index_meta() const { return index_meta_; }

  const std::vector<FieldMeta> &field_metas() const { return index_meta_.field_metas(); }

  /**
   * @brief 插入一条数据
   *
   * @param record 插入的记录，当前假设记录是定长的
   * @param[out] rid    插入的记录的位置
   */
  virtual RC insert_entry(const char *record, const RID *rid) = 0;

  /**
   * @brief 删除一条数据
   *
   * @param record 删除的记录，当前假设记录是定长的
   * @param[in] rid   删除的记录的位置
   */
  virtual RC delete_entry(const char *record, const RID *rid) = 0;

  virtual RC update_entry(const char *old_record, const char *new_record, const RID *rid) = 0;

  /**
   * @brief 创建一个索引数据的扫描器
   *
   * @param left_keys 要扫描的左边界
   * @param left_inclusive 是否包含左边界
   * @param right_keys 要扫描的右边界
   * @param right_inclusive 是否包含右边界
   */
  virtual IndexScanner *create_scanner(const std::vector<IndexUserKey> &left_keys, bool left_inclusive,
      const std::vector<IndexUserKey> &right_keys, bool right_inclusive) = 0;

  /**
   * @brief 同步索引数据到磁盘
   *
   */
  virtual RC sync() = 0;

protected:
  RC init(const IndexMeta &index_meta);

  virtual RC make_user_keys(const char *record, std::vector<IndexUserKey> &user_keys) = 0;

protected:
  IndexMeta index_meta_;  ///< 索引的元数据
};

/**
 * @brief 索引扫描器
 * @ingroup Index
 */
class IndexScanner
{
public:
  IndexScanner()          = default;
  virtual ~IndexScanner() = default;

  /**
   * 遍历元素数据
   * 如果没有更多的元素，返回RECORD_EOF
   */
  virtual RC next_entry(RID *rid) = 0;
  virtual RC destroy()            = 0;
};

class IndexUserKey
{
public:
  IndexUserKey(const Value &value)
      : data_(new char[value.data_length() + KEY_NULL_BYTE]), len_(value.data_length() + KEY_NULL_BYTE)
  {
    memset(data_, value.is_null(), KEY_NULL_BYTE);
    memcpy(data_ + KEY_NULL_BYTE, value.data(), len_ - KEY_NULL_BYTE);
  }
  IndexUserKey(const char *data, size_t len) : data_(new char[len]), len_(len) { memcpy(data_, data, len); }
  IndexUserKey(const IndexUserKey &other) : data_(new char[other.len_]), len_(other.len_)
  {
    memcpy(data_, other.data_, len_);
  }
  ~IndexUserKey() { delete[] data_; }

  const char *data() const { return data_; }
  size_t      len() const { return len_; }

private:
  char  *data_ = nullptr;
  size_t len_  = 0;
};
