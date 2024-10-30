#include <unistd.h>
#include <fcntl.h>
#include "text_utils.h"
#include "common/log/log.h"
#include "common/rc.h"

RC TextUtils::load_text(const Table *table, TextData *data)
{
  std::string text_data_file = table->text_data_file();
  int         fd             = open(text_data_file.c_str(), O_RDONLY);
  if (fd < 0) {
    LOG_WARN("failed to open table text data file %s: %s", text_data_file.c_str(), strerror(errno));
    return RC::IOERR_OPEN;
  }
  lseek(fd, data->offset, SEEK_SET);
  size_t offset = 0;
  auto   buffer = new char[data->len + 1];
  while (offset < data->len) {
    size_t readed = read(fd, buffer + offset, data->len - offset);
    if (readed < 0) {
      LOG_WARN("failed to read text data: %s", strerror(errno));
    }
    offset += readed;
  }
  close(fd);
  buffer[data->len] = '\0';
  data->str         = buffer;
  return RC::SUCCESS;
}
RC TextUtils::dump_text(const Table *table, TextData *data)
{
  // 在文件末尾追加写入 text
  std::string text_data_file = table->text_data_file();
  int         fd             = open(text_data_file.c_str(), O_RDWR);
  if (fd < 0) {
    LOG_WARN("failed to open table text data file %s: %s", text_data_file.c_str(), strerror(errno));
    return RC::IOERR_OPEN;
  }
  size_t end    = lseek(fd, 0, SEEK_END);
  size_t offset = 0;
  while (offset < data->len) {
    size_t writed = write(fd, data->str + offset, data->len - offset);
    if (writed < 0) {
      LOG_WARN("failed to write text data %s", strerror(errno));
      return RC::IOERR_WRITE;
    }
    offset += writed;
  }
  close(fd);
  data->offset = end;
  return RC::SUCCESS;
}
