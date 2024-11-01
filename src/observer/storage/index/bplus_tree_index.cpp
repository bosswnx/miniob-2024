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

#include "storage/index/bplus_tree_index.h"
#include "common/log/log.h"
#include "storage/table/table.h"
#include "storage/db/db.h"
#include <vector>

BplusTreeIndex::~BplusTreeIndex() noexcept { close(); }

RC BplusTreeIndex::create(Table *table, const std::string &file_name, const IndexMeta &index_meta)
{
  if (inited_) {
    LOG_WARN("Failed to create index due to the index has been created before. file_name:%s, index:%s",
        file_name.c_str(), index_meta.name().c_str());
    return RC::RECORD_OPENNED;
  }

  Index::init(index_meta);

  BufferPoolManager &bpm = table->db()->buffer_pool_manager();
  std::vector<AttrType> attr_types;
  std::vector<int>      attr_lengths;
  for (const auto &field_meta : field_metas()) {
    attr_types.push_back(field_meta.type());
    attr_lengths.push_back(field_meta.len());
  }
  RC rc = index_handler_.create(table->db()->log_handler(), bpm, file_name, attr_types, attr_lengths);
  if (RC::SUCCESS != rc) {
    LOG_WARN("Failed to create index_handler, file_name:%s, index:%s, rc:%s",
        file_name.c_str(), index_meta.name().c_str(), strrc(rc));
    return rc;
  }

  inited_ = true;
  table_  = table;
  LOG_INFO("Successfully create index, file_name:%s, index:%s",
    file_name.c_str(), index_meta.name().c_str());
  return RC::SUCCESS;
}

RC BplusTreeIndex::open(Table *table, const std::string &file_name, const IndexMeta &index_meta)
{
  if (inited_) {
    LOG_WARN("Failed to open index due to the index has been initedd before. file_name:%s, index:%s",
        file_name.c_str(), index_meta.name().c_str());
    return RC::RECORD_OPENNED;
  }

  Index::init(index_meta);

  BufferPoolManager &bpm = table->db()->buffer_pool_manager();
  RC rc = index_handler_.open(table->db()->log_handler(), bpm, file_name);
  if (RC::SUCCESS != rc) {
    LOG_WARN("Failed to open index_handler, file_name:%s, index:%s, rc:%s",
        file_name.c_str(), index_meta.name().c_str(), strrc(rc));
    return rc;
  }

  inited_ = true;
  table_  = table;
  LOG_INFO("Successfully open index, file_name:%s, index:%s",
    file_name.c_str(), index_meta.name().c_str());
  return RC::SUCCESS;
}

RC BplusTreeIndex::close()
{
  if (inited_) {
    LOG_INFO("Begin to close index, index:%s", index_meta_.name().c_str());
    index_handler_.close();
    inited_ = false;
  }
  LOG_INFO("Successfully close index.");
  return RC::SUCCESS;
}

RC BplusTreeIndex::insert_entry(const char *record, const RID *rid)
{
  std::vector<const char *> user_keys;
  for (const auto &field_meta : field_metas()) {
    auto user_key = record + field_meta.offset();
    user_keys.push_back(user_key);
  }
  return index_handler_.insert_entry(user_keys, rid);
}

RC BplusTreeIndex::delete_entry(const char *record, const RID *rid)
{
  std::vector<const char *> user_keys;
  for (const auto &field_meta : field_metas()) {
    auto user_key = record + field_meta.offset();
    user_keys.push_back(user_key);
  }
  return index_handler_.delete_entry(user_keys, rid);
}

IndexScanner *BplusTreeIndex::create_scanner(const std::vector<const char *> &left_keys, bool left_inclusive,
    const std::vector<const char *> &right_keys, bool right_inclusive)
{
  BplusTreeIndexScanner *index_scanner = new BplusTreeIndexScanner(index_handler_);
  RC                     rc            = index_scanner->open(left_keys, left_inclusive, right_keys, right_inclusive);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open index scanner. rc=%d:%s", rc, strrc(rc));
    delete index_scanner;
    return nullptr;
  }
  return index_scanner;
}

RC BplusTreeIndex::sync() { return index_handler_.sync(); }

////////////////////////////////////////////////////////////////////////////////
BplusTreeIndexScanner::BplusTreeIndexScanner(BplusTreeHandler &tree_handler) : tree_scanner_(tree_handler) {}

BplusTreeIndexScanner::~BplusTreeIndexScanner() noexcept { tree_scanner_.close(); }

RC BplusTreeIndexScanner::open(const std::vector<const char *> &left_keys, bool left_inclusive,
    const std::vector<const char *> &right_keys, bool right_inclusive)
{
  return tree_scanner_.open(left_keys, left_inclusive, right_keys, right_inclusive);
}

RC BplusTreeIndexScanner::next_entry(RID *rid) { return tree_scanner_.next_entry(*rid); }

RC BplusTreeIndexScanner::destroy()
{
  delete this;
  return RC::SUCCESS;
}
