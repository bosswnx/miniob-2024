#pragma once
#include <unordered_map>
#include <list>
#include <unordered_set>
#include <memory>

#include "common/rc.h"
#include "common/type/attr_type.h"

class Table;

/// 为了减少系统调用次数、减少文件 IO 次数，使用 VectorDataManager 读写向量数据
/// 主要逻辑： 1. IO 规模规整化为磁盘物理页大小  2. 使用缓冲池减少IO次数   3. LRU管理缓冲
class VectorDataManager
{
  // 向量数据文件按页管理, 每 4K 即 Page, Page加载到内存后形成 Buffer, Buffer 经历若干次读写后写回 Page
private:
  using PageId                      = ssize_t;
  static constexpr size_t PAGE_SIZE = 4096;

  explicit VectorDataManager(int fd, PageId max_page_id)
      : fd_(fd), max_page_id_(max_page_id), data_end_((max_page_id + 1) * PAGE_SIZE)
  {}

  RC get_buffer(PageId page_id, std::byte *&buffer);

  RC alloc_buffer(PageId page_id, std::byte *&buffer);

  RC release_buffer(PageId page_id);

public:
  static std::unique_ptr<VectorDataManager> create(const Table *table);

  RC load_vector(VectorData *vector_data);
  RC dump_vector(VectorData *vector_data);
  RC update_vector(const VectorData *old_vector_data, const VectorData *new_vector_data);

  ~VectorDataManager();

private:
  int                                     fd_;
  PageId                                  max_page_id_;
  std::unordered_map<PageId, std::byte *> buffer_pool_;
  std::unordered_set<PageId>              dirty_buffers_;

  /// NOTE: 目前的实现 每次重启数据库后（如果向量已经持久化）data_end_从页头开始，会造成数据空洞

  size_t                  data_end_;  // 文件的data_end_之前是向量数据，之后 (包括自己）是空数据
  std::list<PageId>       lru_list_;  // 链表头表示最近经常使用，末尾表示最近最少使用
  static constexpr size_t BUFFER_POOL_SIZE = 16;
};