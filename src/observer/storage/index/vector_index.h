#pragma once

#include "storage/record/record.h"
#include "storage/index/vector_index_meta.h"

class VectorIndex
{
public:
  VectorIndex(const VectorIndexMeta &meta, size_t dim, DistanceType distance_type, size_t lists, size_t probes,
      const std::string &filename);
  ~VectorIndex();

  void add_item(RID rid, const float *vector);
  RC build_and_save();
  // RC   load();
  void query(const float *w, size_t n, std::vector<RID> &result, std::vector<float> &distance);
  [[nodiscard]] const VectorIndexMeta &meta() const;

private:
  VectorIndexMeta    meta_;
  const size_t       dim_;
  const DistanceType distance_type_;
  const size_t       lists_;
  const size_t       probes_;
  void              *index_;
  int                item_count_ = 0;
  const std::string  filename_;
  std::ofstream      aux_file_;
  RID               *rid_map_start = nullptr;
};