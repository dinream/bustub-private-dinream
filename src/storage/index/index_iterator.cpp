/**
 * index_iterator.cpp
 */
#include <cassert>
#include <utility>

#include "common/config.h"
#include "storage/index/index_iterator.h"
#include "storage/page/hash_table_page_defs.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(ReadPageGuard *cur_pg, int cur_idx, const KeyComparator &comparator,
                                  BufferPoolManager *bpm, INDEXITERATOR_TYPE *end_iterator)
    : cur_pg_(cur_pg), cur_idx_(cur_idx), comparator_(comparator), bpm_(bpm), end_iterator_(end_iterator) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  // throw std::runtime_error("unimplemented");
  return cur_pg_ == nullptr && cur_idx_ == 0;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  // throw std::runtime_error("unimplemented");
  auto cur_page = cur_pg_->As<LeafPage>();
  MappingType mp = std::make_pair(cur_page->KeyAt(cur_idx_), cur_page->ValueAt(cur_idx_));
  return std::move(mp);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  // throw std::runtime_error("unimplemented");
  auto cur_page = cur_pg_->As<LeafPage>();
  cur_idx_++;
  if (cur_idx_ >= cur_page->GetSize()) {
    page_id_t cur_id = cur_page->GetNextPageId();
    if (cur_id == INVALID_PAGE_ID) {
      cur_pg_ = nullptr;
    } else {
      auto new_pg = bpm_->FetchPageRead(cur_page->GetNextPageId());
      *cur_pg_ = std::move(new_pg);
    }
    cur_idx_ = 0;
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
