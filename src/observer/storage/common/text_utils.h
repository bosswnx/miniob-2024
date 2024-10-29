#pragma once
#include "storage/table/table.h"

namespace TextUtils {
RC load_text(const Table *table, TextData *data);
RC dump_text(const Table *table, TextData *data);
}  // namespace TextUtils