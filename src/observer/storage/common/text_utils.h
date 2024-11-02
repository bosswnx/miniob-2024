#pragma once
#include "storage/table/table.h"

namespace TextUtils {
RC load_text(const Table *table, TextData *data);
RC dump_text(const Table *table, TextData *data);
}  // namespace TextUtils

namespace VectorUtils {
RC load_vector(const Table *table, VectorData *data);
RC dump_vector(const Table *table, VectorData *data);
}  // namespace VectorUtils