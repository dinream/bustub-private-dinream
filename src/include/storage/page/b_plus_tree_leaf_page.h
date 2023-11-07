//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_leaf_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <string>
#include <utility>
#include <vector>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_LEAF_PAGE_TYPE BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>
#define LEAF_PAGE_HEADER_SIZE 16
#define LEAF_PAGE_SIZE ((BUSTUB_PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / sizeof(MappingType))

/**
 * Store indexed key and record id(record id = page id combined with slot id,
 * see include/common/rid.h for detailed implementation) together within leaf
 * page. Only support unique key.
 *
 * Leaf page format (keys are stored in order):
 *  ----------------------------------------------------------------------
 * | HEADER | KEY(1) + RID(1) | KEY(2) + RID(2) | ... | KEY(n) + RID(n)
 *  ----------------------------------------------------------------------
 *
 *  Header format (size in byte, 16 bytes in total):
 *  ---------------------------------------------------------------------
 * | PageType (4) | CurrentSize (4) | MaxSize (4) |
 *  ---------------------------------------------------------------------
 *  -----------------------------------------------
 * |  NextPageId (4)
 *  -----------------------------------------------
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeLeafPage : public BPlusTreePage {
 public:
  // Delete all constructor / destructor to ensure memory safety
  BPlusTreeLeafPage() = delete;
  BPlusTreeLeafPage(const BPlusTreeLeafPage &other) = delete;

  /**
   * After creating a new leaf page from buffer pool, must call initialize
   * method to set default values
   * @param max_size Max size of the leaf node
   */
  void Init(int max_size = LEAF_PAGE_SIZE);

  // helper methods
  auto GetNextPageId() const -> page_id_t;
  void SetNextPageId(page_id_t next_page_id);
  auto KeyAt(int index) const -> KeyType;
  // 下面三个 是新加的
  void SetKeyAt(int index, const KeyType &key);

  /**
   *
   * @param value the value to search for
   */
  auto ValueIndex(const ValueType &value) const -> int;

  /**
   *
   * @param index the index
   * @return the value at the index
   */
  auto ValueAt(int index) const -> ValueType;
  void SetValueAt(int index, const ValueType &value);
  void InsertIndex(int index, KeyType key, ValueType value);

  /**
   * 不管 index 到底是不是我的元素->--> index，必定是 search 的结果 并且这个 index 结果必定是需要已经检查过的，，即
   * 我只负责删除
   * @param index The index of the key to delete. Index must be non-zero.
   * @return Key at index
   */
  void DeleteIndex(int index);

  /**
   * @param key The key of the array to search. key must not be exist.
   * @return Key at index
   */
  auto SearchKey(KeyType key, const KeyComparator &comparator, int &idx) const -> bool;

  auto Divid2Other(B_PLUS_TREE_LEAF_PAGE_TYPE &other) -> MappingType;
  auto Merge2Other(B_PLUS_TREE_LEAF_PAGE_TYPE &other) -> MappingType;
  auto PairAt(int idx) const -> const MappingType &;

  /**
   * @brief for test only return a string representing all keys in
   * this leaf page formatted as "(key1,key2,key3,...)"
   *
   * @return std::string
   */
  auto ToString() const -> std::string {
    std::string kstr = "(";
    bool first = true;

    for (int i = 0; i < GetSize(); i++) {
      KeyType key = KeyAt(i);
      if (first) {
        first = false;
      } else {
        kstr.append(",");
      }

      kstr.append(std::to_string(key.ToString()));
    }
    kstr.append(")");

    return kstr;
  }

 private:
  page_id_t next_page_id_;
  // Flexible array member for page data.
  MappingType array_[0];
};
}  // namespace bustub
