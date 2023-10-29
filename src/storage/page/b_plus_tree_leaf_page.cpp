//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstddef>
#include <sstream>

#include "common/config.h"
#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/hash_table_page_defs.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t {
  return next_page_id_;
  // return INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  KeyType key{array_[index].first};
  return key;
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  // if(index > =size_)return ;
  array_[index].first = key;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  if (index >= GetSize()) {
    return ValueType{};  // 返回一个空的值
  }
  ValueType val{array_[index].second};
  return val;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::InsertIndex(int index, KeyType key, ValueType value) {
  if (index < 0 || index > GetSize()) {
    return;  // index 越界
  }
  SetSize(GetSize() + 1);  // 最大大小减一
  auto array = new MappingType[GetSize()];
  for (int i = 0; i < index; i++) {
    array[i].first = array_[i].first;
    array[i].second = array_[i].second;
  }
  array[index].first = key;
  array[index].second = value;
  for (int i = index + 1; i < GetSize(); i++) {
    array[i].first = array_[i - 1].first;
    array[i].second = array_[i - 1].second;
  }
  array_ = array;
  delete[] array_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::DeleteIndex(int index) {
  if (index < 0 || index > GetSize() - 1) {
    return;  // index 越界
  }
  SetSize(GetSize() - 1);  // 最大大小减一
  auto array = new MappingType[GetSize()];
  for (int i = 0; i < index; i++) {
    array[i].first = array_[i].first;
    array[i].second = array_[i].second;
  }
  for (int i = index; i < GetSize(); i++) {
    array[i].first = array_[i + 1].first;
    array[i].second = array_[i + 1].second;
  }
  array_ = array;
  delete[] array_;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::SearchKey(KeyType key, const KeyComparator &comparator, int &idx) const -> bool {
  int min = 0;
  int max = GetSize() - 1;
  while (min <= max) {
    int mid = (min + max) / 2;
    if (comparator(key, array_[mid].first) < 0) {
      max = mid - 1;
    } else if (comparator(key, array_[mid].first) > 0) {
      min = mid + 1;
    } else {
      idx = mid;
      return true;
    }
  }
  // min 即为最终的位置的左侧
  idx = min;
  return false;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Divid2Other(B_PLUS_TREE_LEAF_PAGE_TYPE &other) -> MappingType {
  other.Init(GetMaxSize());
  int j = 0;
  int n = GetSize() / 2;
  for (int i = n; i <= GetMaxSize(); i++) {
    other.InsertIndex(j++, KeyAt(n), ValueAt(n));
    DeleteIndex(i);
  }
  return {other.KeyAt(0), other.ValueAt(0)};
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Merge2Other(B_PLUS_TREE_LEAF_PAGE_TYPE &other) -> MappingType {
  // 大的合并到小的身上， other 为 小
  int j = other.GetSize();
  int n = GetSize();
  other.SetNextPageId(GetNextPageId());
  for (int i = 0; i < n; i++) {
    other.InsertIndex(j++, KeyAt(0), ValueAt(0));
    DeleteIndex(0);
  }
  return {other.KeyAt(0), other.ValueAt(0)};
}
template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
