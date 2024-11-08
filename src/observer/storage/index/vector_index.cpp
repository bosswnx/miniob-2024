#include "vector_index.h"
#include "deps/3rd/annoy/src/annoylib.h"
#include "deps/3rd/annoy/src/kissrandom.h"
template <typename T>
using MiniObAnnoyIndex =
    Annoy::AnnoyIndex<int, float, T, Annoy::Kiss32Random, Annoy::AnnoyIndexSingleThreadedBuildPolicy>;

VectorIndex::VectorIndex(const VectorIndexMeta &meta, size_t dim, DistanceType distance_type, size_t lists,
    size_t probes, const std::string &filename)
    : meta_(meta), dim_(dim), distance_type_(distance_type), lists_(lists), probes_(probes), filename_(filename)
{
  switch (distance_type_) {
    case DistanceType::L2_DISTANCE: {
      index_ = new MiniObAnnoyIndex<Annoy::Euclidean>(dim);
      break;
    }
    case DistanceType::COSINE_DISTANCE: {
      index_ = new MiniObAnnoyIndex<Annoy::Angular>(dim);
      break;
    }
    case DistanceType::INNER_PRODUCT: {
      index_ = new MiniObAnnoyIndex<Annoy::DotProduct>(dim);
      break;
    }
    default: ASSERT(false, "not implemented");
  }
  aux_file_.open(filename_ + ".aux", std::ios::binary);
}

VectorIndex::~VectorIndex()
{
  switch (distance_type_) {
    case DistanceType::L2_DISTANCE: {
      const auto index = static_cast<MiniObAnnoyIndex<Annoy::Euclidean> *>(index_);
      delete index;
      break;
    }
    case DistanceType::COSINE_DISTANCE: {
      const auto index = static_cast<MiniObAnnoyIndex<Annoy::Angular> *>(index_);
      delete index;
      break;
    }
    case DistanceType::INNER_PRODUCT: {
      const auto index = static_cast<MiniObAnnoyIndex<Annoy::DotProduct> *>(index_);
      delete index;
      break;
    }
    default: ASSERT(false, "not implemented");
  }
  munmap(rid_map_start, sizeof(RID) * item_count_);
}

void VectorIndex::add_item(RID rid, const float *vector)
{
  switch (distance_type_) {
    case DistanceType::L2_DISTANCE: {
      const auto index = static_cast<MiniObAnnoyIndex<Annoy::Euclidean> *>(index_);
      index->add_item(item_count_++, vector);
      break;
    }
    case DistanceType::COSINE_DISTANCE: {
      const auto index = static_cast<MiniObAnnoyIndex<Annoy::Angular> *>(index_);
      index->add_item(item_count_++, vector);
      break;
    }
    case DistanceType::INNER_PRODUCT: {
      const auto index = static_cast<MiniObAnnoyIndex<Annoy::DotProduct> *>(index_);
      index->add_item(item_count_++, vector);
      break;
    }
    default: ASSERT(false, "not implemented");
  }
  aux_file_.write(reinterpret_cast<char *>(&rid), sizeof(RID));
}

RC VectorIndex::build_and_save()
{
  // 没有数据时，也能建立索引，但无法查询出数据
  if (item_count_ == 0) {
    return RC::SUCCESS;
  }
  aux_file_.close();
  switch (distance_type_) {
    case DistanceType::L2_DISTANCE: {
      const auto index = static_cast<MiniObAnnoyIndex<Annoy::Euclidean> *>(index_);
      index->build(lists_);
      index->save(filename_.c_str());
      break;
    }
    case DistanceType::COSINE_DISTANCE: {
      const auto index = static_cast<MiniObAnnoyIndex<Annoy::Angular> *>(index_);
      index->build(lists_);
      index->save(filename_.c_str());
      break;
    }
    case DistanceType::INNER_PRODUCT: {
      const auto index = static_cast<MiniObAnnoyIndex<Annoy::DotProduct> *>(index_);
      index->build(lists_);
      index->save(filename_.c_str());
      break;
    }
    default: ASSERT(false, "not implemented");
  }
  std::string aux_file = filename_ + ".aux";
  int         fd       = open(aux_file.c_str(), O_RDONLY);
  if (fd < 0) {
    LOG_WARN("failed to open %s: %s", aux_file.c_str(), strerror(errno));
    return RC::IOERR_OPEN;
  }
  void *map_start = mmap(nullptr, sizeof(RID) * item_count_, PROT_READ, MAP_PRIVATE, fd, 0);
  if (map_start == MAP_FAILED) {
    LOG_WARN("failed to mmap %s: %s", aux_file.c_str(), strerror(errno));
    return RC::IOERR_ACCESS;
  }
  rid_map_start = static_cast<RID *>(map_start);
  return RC::SUCCESS;
}

// RC VectorIndex::load()
// {
//   switch (distance_type_) {
//     case DistanceType::L2_DISTANCE: {
//       const auto index = static_cast<MiniObAnnoyIndex<Annoy::Euclidean> *>(index_);
//       index->load(filename_.c_str());
//       break;
//     }
//     case DistanceType::COSINE_DISTANCE: {
//       const auto index = static_cast<MiniObAnnoyIndex<Annoy::Angular> *>(index_);
//       index->load(filename_.c_str());
//       break;
//     }
//     case DistanceType::INNER_PRODUCT: {
//       const auto index = static_cast<MiniObAnnoyIndex<Annoy::DotProduct> *>(index_);
//       index->load(filename_.c_str());
//       break;
//     }
//     default: ASSERT(false, "not implemented");
//   }
// }

void VectorIndex::query(const float *w, size_t n, std::vector<RID> &result, std::vector<float> &distance)
{
  if (item_count_ == 0) {
    return;
  }
  std::vector<int> result_internal;
  switch (distance_type_) {
    case DistanceType::L2_DISTANCE: {
      const auto index = static_cast<MiniObAnnoyIndex<Annoy::Euclidean> *>(index_);
      index->get_nns_by_vector(w, n, probes_, &result_internal, &distance);
      break;
    }
    case DistanceType::COSINE_DISTANCE: {
      const auto index = static_cast<MiniObAnnoyIndex<Annoy::Angular> *>(index_);
      index->get_nns_by_vector(w, n, probes_, &result_internal, &distance);
      break;
    }
    case DistanceType::INNER_PRODUCT: {
      const auto index = static_cast<MiniObAnnoyIndex<Annoy::DotProduct> *>(index_);
      index->get_nns_by_vector(w, n, probes_, &result_internal, &distance);
      break;
    }
    default: ASSERT(false, "not implemented");
  }
  result.reserve(result_internal.size());
  for (const int i : result_internal) {
    result.push_back(rid_map_start[i]);
  }
}

const VectorIndexMeta &VectorIndex::meta() const { return meta_; }
