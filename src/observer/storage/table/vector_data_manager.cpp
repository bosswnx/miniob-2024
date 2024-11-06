#include <fcntl.h>
#include <cmath>

#include "storage/table/vector_data_manager.h"
#include "storage/table/table.h"
#include "common/log/log.h"

RC VectorDataManager::get_buffer(PageId page_id, std::byte *&buffer)
{
  LOG_TRACE("get buffer %d", page_id);
  RC   rc = RC::SUCCESS;
  auto it = buffer_pool_.find(page_id);
  if (it != buffer_pool_.end()) {
    lru_list_.erase(std::find(lru_list_.begin(), lru_list_.end(), page_id));
    lru_list_.push_front(page_id);
    buffer = it->second;
    return rc;
  }
  rc = alloc_buffer(page_id, buffer);
  if (OB_FAIL(rc)) {
    LOG_WARN("failed to allocate a new buffer: %s", strrc(rc));
    return rc;
  }

  buffer_pool_[page_id] = buffer;
  lru_list_.push_front(page_id);
  if (lru_list_.size() > BUFFER_POOL_SIZE) {  // 淘汰最近最少使用的 page
    ASSERT(lru_list_.size() - 1 == BUFFER_POOL_SIZE, "not implemented");
    release_buffer(lru_list_.back());
  }
  return RC::SUCCESS;
}

RC VectorDataManager::alloc_buffer(PageId page_id, std::byte *&buffer)
{
  // 开辟一个全新的page
  if (page_id > max_page_id_) {
    LOG_TRACE("alloc a new buffer %d", page_id);
    ASSERT(page_id == max_page_id_ + 1, "not implemented");
    max_page_id_++;
    if (ftruncate(fd_, (max_page_id_ + 1) * PAGE_SIZE) < 0) {
      LOG_WARN("failed to ftruncate: %s", strerror(errno));
      return RC::IOERR_ACCESS;
    }
    buffer = new std::byte[PAGE_SIZE];
    return RC::SUCCESS;
  }
  LOG_TRACE("alloc an old buffer %d", page_id);
  // 将已有的 page 加载进内存形成 buffer
  if (lseek(fd_, page_id * PAGE_SIZE, SEEK_SET) == static_cast<off_t>(-1)) {
    LOG_WARN("failed to lseek: %s", strerror(errno));
    return RC::IOERR_ACCESS;
  }
  size_t offset = 0;
  buffer        = new std::byte[PAGE_SIZE];

  while (offset < PAGE_SIZE) {
    ssize_t readed = read(fd_, buffer + offset, PAGE_SIZE - offset);
    if (readed < 0) {
      LOG_WARN("failed to read vector data: %s", strerror(errno));
      delete[] buffer;
      buffer = nullptr;
      return RC::IOERR_READ;
    }
    offset += readed;
  }
  return RC::SUCCESS;
}

RC VectorDataManager::release_buffer(PageId page_id)
{
  LOG_TRACE("release buffer %d", page_id);
  ASSERT(buffer_pool_.contains(page_id), "internal error");
  if (const auto it = dirty_buffers_.find(page_id); it != dirty_buffers_.end()) {
    if (lseek(fd_, page_id * PAGE_SIZE, SEEK_SET) == static_cast<off_t>(-1)) {
      LOG_WARN("failed to lseek: %s", strerror(errno));
      return RC::IOERR_ACCESS;
    }
    size_t offset = 0;
    while (offset < PAGE_SIZE) {
      ssize_t writed = write(fd_, buffer_pool_[page_id] + offset, PAGE_SIZE - offset);
      if (writed < 0) {
        LOG_WARN("failed to write back buffer: %s", strerror(errno));
        return RC::IOERR_WRITE;
      }
      offset += writed;
    }
    dirty_buffers_.erase(it);
  }
  delete[] buffer_pool_[page_id];
  buffer_pool_.erase(page_id);
  lru_list_.pop_back();
  return RC::SUCCESS;
}

std::unique_ptr<VectorDataManager> VectorDataManager::create(const Table *table)
{
  std::string vector_data_file = table->vector_data_file();
  int         fd               = open(vector_data_file.c_str(), O_RDWR | O_CREAT, 0600);
  if (fd < 0) {
    LOG_WARN("failed to open file %s : %s", vector_data_file.c_str(), strerror(errno));
    return nullptr;
  }
  size_t file_size   = lseek(fd, 0, SEEK_END);
  PageId max_page_id = std::ceil(file_size / static_cast<float>(PAGE_SIZE)) - 1;
  return std::unique_ptr<VectorDataManager>(new VectorDataManager(fd, max_page_id));
}

RC VectorDataManager::load_vector(VectorData *vector_data)
{
  LOG_TRACE("load vector[offset = %d, dim = %d]", vector_data->offset, vector_data->dim);
  PageId page_id_start = std::floor(vector_data->offset / static_cast<float>(PAGE_SIZE));
  PageId page_id_end =
      std::floor((vector_data->offset + vector_data->dim * sizeof(float)) / static_cast<float>(PAGE_SIZE));
  const auto vector           = new float[vector_data->dim];
  size_t     copy_dest_offset = 0;
  for (PageId current_page_id = page_id_start; current_page_id <= page_id_end; current_page_id += 1) {
    std::byte *buffer;
    if (RC rc = get_buffer(current_page_id, buffer); OB_FAIL(rc)) {
      delete[] vector;
      LOG_WARN("failed to load vector: %s", strrc(rc));
      return rc;
    }

    void  *copy_start;
    size_t max_copy_len;
    if (vector_data->offset > current_page_id * PAGE_SIZE) {
      copy_start = buffer + vector_data->offset % PAGE_SIZE;  // 复制第一页时，从offset 开始而不是从页首地址开始
      max_copy_len = PAGE_SIZE - vector_data->offset % PAGE_SIZE;
    } else {
      copy_start   = buffer;
      max_copy_len = PAGE_SIZE;
    }
    // 复制数据可能横跨多页，也有可能不足一页
    const size_t copy_len = std::min(vector_data->dim * sizeof(float) - copy_dest_offset, max_copy_len);
    memcpy(reinterpret_cast<std::byte *>(vector) + copy_dest_offset, copy_start, copy_len);
    copy_dest_offset += copy_len;
  }
  vector_data->vector = vector;
  return RC::SUCCESS;
}

RC VectorDataManager::dump_vector(VectorData *vector_data)
{
  LOG_TRACE("dump vector [%f, %f, %f...]", vector_data->vector[0], vector_data->vector[1], vector_data->vector[2]);
  PageId page_id_start   = std::floor(data_end_ / static_cast<float>(PAGE_SIZE));
  PageId page_id_end     = std::floor((data_end_ + vector_data->dim * sizeof(float)) / static_cast<float>(PAGE_SIZE));
  PageId copy_src_offset = 0;
  for (PageId current_page_id = page_id_start; current_page_id <= page_id_end; current_page_id += 1) {
    std::byte *buffer;
    if (RC rc = get_buffer(current_page_id, buffer); OB_FAIL(rc)) {
      LOG_WARN("failed to load vector: %s", strrc(rc));
      return rc;
    }
    void  *copy_start;
    size_t max_copy_len;
    if (data_end_ > max_page_id_ * PAGE_SIZE) {
      copy_start          = buffer + data_end_ % PAGE_SIZE;
      vector_data->offset = data_end_;
      max_copy_len        = PAGE_SIZE - data_end_ % PAGE_SIZE;
    } else {
      copy_start   = buffer;
      max_copy_len = PAGE_SIZE;
    }
    const size_t copy_len = std::min(vector_data->dim * sizeof(float) - copy_src_offset, max_copy_len);
    memcpy(copy_start, reinterpret_cast<const std::byte *>(vector_data->vector) + copy_src_offset, copy_len);
    copy_src_offset += copy_len;
    dirty_buffers_.insert(current_page_id);
  }
  data_end_ += vector_data->dim * sizeof(float);
  return RC::SUCCESS;
}

RC VectorDataManager::update_vector(const VectorData *old_vector_data, const VectorData *new_vector_data)
{
  ASSERT(old_vector_data->dim == new_vector_data->dim, "only vectors with the same dimension can be updated");
  PageId page_id_start = std::floor(old_vector_data->offset / static_cast<float>(PAGE_SIZE));
  PageId page_id_end =
      std::floor((old_vector_data->offset + old_vector_data->dim * sizeof(float)) / static_cast<float>(PAGE_SIZE));
  size_t copy_src_offset = 0;
  for (PageId current_page_id = page_id_start; current_page_id <= page_id_end; current_page_id += 1) {
    std::byte *buffer;
    if (RC rc = get_buffer(current_page_id, buffer); OB_FAIL(rc)) {
      LOG_WARN("failed to load vector: %s", strrc(rc));
      return rc;
    }

    void  *copy_start;
    size_t max_copy_len;
    if (old_vector_data->offset > current_page_id * PAGE_SIZE) {
      copy_start = buffer + old_vector_data->offset % PAGE_SIZE;  // 复制第一页时，从offset 开始而不是从页首地址开始
      max_copy_len = PAGE_SIZE - old_vector_data->offset % PAGE_SIZE;
    } else {
      copy_start   = buffer;
      max_copy_len = PAGE_SIZE;
    }
    // 复制数据可能横跨多页，也有可能不足一页
    const size_t copy_len = std::min(old_vector_data->dim * sizeof(float) - copy_src_offset, max_copy_len);
    memcpy(copy_start, reinterpret_cast<const std::byte *>(new_vector_data->vector) + copy_src_offset, copy_len);
    copy_src_offset += copy_len;
    dirty_buffers_.insert(current_page_id);
  }
  return RC::SUCCESS;
}

VectorDataManager::~VectorDataManager()
{
  close(fd_);
  for (auto &[page_id, buffer] : buffer_pool_) {
    delete[] buffer;
  }
}
