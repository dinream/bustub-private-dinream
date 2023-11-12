//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/hash_table_page_defs.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, and set max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  // array_=malloc(size+newsize); 这里的申请是需要在外面用的
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  KeyType key{array_[index].first};
  return key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  // if(index > =size_)return ;
  array_[index].first = key;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  if (index >= GetSize()) {
    return INVALID_PAGE_ID;
  }
  ValueType val{array_[index].second};
  return val;
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  // if(index > =size_)return ;
  array_[index].second = value;
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertIndex(int index, KeyType key, ValueType value) {
  if (index < 0 || index > GetSize()) {
    return;  // index 越界
  }
  SetSize(GetSize() + 1);  // 最大大小+一
  for (int i = GetSize() - 1; i > index; i--) {
    array_[i] = array_[i - 1];
  }
  array_[index] = std::make_pair(key, value);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::DeleteIndex(int index) {
  if (index < 0 || index > GetSize() - 1) {
    return;  // index 越界
  }
  for (int i = index; i < GetSize() - 1; i++) {
    array_[i] = array_[i + 1];
  }
  SetSize(GetSize() - 1);
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::SearchKey(KeyType key, const KeyComparator &comparator, int &idx) const -> bool {
  int min = 1;
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
  // min 即为最终的位置
  idx = max;
  return false;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Divid2Other(B_PLUS_TREE_INTERNAL_PAGE_TYPE &other) -> MappingType {
  int j = 0;
  int n = (GetSize() + 1) / 2;
  other.SetSize(GetSize() - n);
  for (int i = n; i <= GetMaxSize(); i++) {
    // other.InsertIndex(j++, KeyAt(n), ValueAt(n));
    other.SetKeyAt(j, KeyAt(i));
    other.SetValueAt(j, ValueAt(i));
    // DeleteIndex(n);
    j++;
  }
  SetSize(n);
  return {other.KeyAt(0), other.ValueAt(0)};
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Merge2Other(B_PLUS_TREE_INTERNAL_PAGE_TYPE &other) -> MappingType {
  // 大的合并到小的身上， other 为 小
  int j = other.GetSize();
  int n = GetSize();
  // for (int i = 0; i < n; i++) {
  //   other.InsertIndex(j++, KeyAt(0), ValueAt(0));
  //   DeleteIndex(0);
  // }
  other.SetSize(GetSize() + other.GetSize());
  for (int i = 0; i < n; i++) {
    other.SetKeyAt(j, KeyAt(i));
    other.SetValueAt(j, ValueAt(i));
    j++;
  }
  SetSize(0);
  return {other.KeyAt(0), other.ValueAt(0)};
}
// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
