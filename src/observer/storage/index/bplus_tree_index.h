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
// Created by wangyunlai.wyl on 2021/5/19.
//

#pragma once

#include "storage/index/bplus_tree.h"
#include "storage/index/index.h"
#include <string>

/**
 * @brief B+树索引
 * @ingroup Index
 */
class BplusTreeIndex : public Index
{
public:
  BplusTreeIndex() = default;
  virtual ~BplusTreeIndex() noexcept;

  RC create(Table *table, const std::string &file_name, const IndexMeta &index_meta);
  RC open(Table *table, const std::string &file_name, const IndexMeta &index_meta);
  RC close();

  RC insert_entry(const char *record, const RID *rid) override;
  RC delete_entry(const char *record, const RID *rid) override;

  RC update_entry(const char *old_record, const char *new_record, const RID *rid) override;

  /**
   * 扫描指定范围的数据
   */
  IndexScanner *create_scanner(const std::vector<IndexUserKey> &left_keys, bool left_inclusive,
      const std::vector<IndexUserKey> &right_keys, bool right_inclusive) override;

  RC sync() override;

protected:
  RC make_user_keys(const char *record, std::vector<IndexUserKey> &user_keys) override;

private:
  bool             inited_ = false;
  Table           *table_  = nullptr;
  BplusTreeHandler index_handler_;
};

/**
 * @brief B+树索引扫描器
 * @ingroup Index
 */
class BplusTreeIndexScanner : public IndexScanner
{
public:
  BplusTreeIndexScanner(BplusTreeHandler &tree_handle);
  ~BplusTreeIndexScanner() noexcept override;

  RC next_entry(RID *rid) override;
  RC destroy() override;

  RC open(const std::vector<IndexUserKey> &left_keys, bool left_inclusive, const std::vector<IndexUserKey> &right_keys,
      bool right_inclusive);

private:
  BplusTreeScanner tree_scanner_;
};
