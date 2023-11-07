//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_delete_test.cpp
//
// Identification: test/storage/b_plus_tree_delete_test.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <cstdint>
#include <cstdio>

#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/index/b_plus_tree.h"
#include "test_util.h"  // NOLINT

namespace bustub {

using bustub::DiskManagerUnlimitedMemory;

TEST(BPlusTreeTests, DISABLED_DeleteTest1) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", header_page->GetPageId(), bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  std::vector<int64_t> remove_keys = {1, 5};
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }

  int64_t size = 0;
  bool is_present;

  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    is_present = tree.GetValue(index_key, &rids);

    if (!is_present) {
      EXPECT_NE(std::find(remove_keys.begin(), remove_keys.end(), key), remove_keys.end());
    } else {
      EXPECT_EQ(rids.size(), 1);
      EXPECT_EQ(rids[0].GetPageId(), 0);
      EXPECT_EQ(rids[0].GetSlotNum(), key);
      size = size + 1;
    }
  }

  EXPECT_EQ(size, 3);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete bpm;
}

TEST(BPlusTreeTests, DISABLED_DeleteTest2) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", header_page->GetPageId(), bpm, comparator);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);

  std::vector<int64_t> keys = {1, 2, 3, 4, 5};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  std::vector<int64_t> remove_keys = {1, 5, 3, 4};
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }

  int64_t size = 0;
  bool is_present;

  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    is_present = tree.GetValue(index_key, &rids);

    if (!is_present) {
      EXPECT_NE(std::find(remove_keys.begin(), remove_keys.end(), key), remove_keys.end());
    } else {
      EXPECT_EQ(rids.size(), 1);
      EXPECT_EQ(rids[0].GetPageId(), 0);
      EXPECT_EQ(rids[0].GetSlotNum(), key);
      size = size + 1;
    }
  }

  EXPECT_EQ(size, 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete bpm;
}
TEST(BPlusTreeTests, DISABLED_DeleteTest3) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", header_page->GetPageId(), bpm, comparator, 3, 5);
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);
  for (int64_t i = 1; i <= 5000; i++) {
    int64_t value = (2 * i) & 0xFFFFFFFF;
    int64_t value2 = (2 * i - 1) & 0xFFFFFFFF;

    rid.Set(static_cast<int32_t>((2 * i) >> 32), value);
    index_key.SetFromInteger((2 * i));
    tree.Insert(index_key, rid, transaction);

    rid.Set(static_cast<int32_t>((2 * i - 1) >> 32), value2);
    index_key.SetFromInteger((2 * i - 1));
    tree.Insert(index_key, rid, transaction);
  }
  for (int64_t i = 1; i <= 5000; i++) {
    if (2 * i - 1 == 551) {
      std::cout << "haha" << std::endl;
    }
    index_key.SetFromInteger(2 * i - 1);
    tree.Remove(index_key, transaction);
  }

  std::vector<int64_t> keys = {1,  2,  5,  4,  3,  9,  8,  7,  6,  8,  9,  12, 11, 10, 21, 22, 19, 20, 18, 17, 16,
                               15, 14, 30, 29, 28, 27, 26, 25, 23, 24, 22, 99, 98, 97, 96, 95, 94, 93, 92, 91, 90};
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }
  tree.DrawBPlusTree();
  std::vector<RID> rids;
  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    tree.GetValue(index_key, &rids);
    // EXPECT_EQ(rids.size(), 1);

    int64_t value = key & 0xFFFFFFFF;
    EXPECT_EQ(rids[0].GetSlotNum(), value);
  }

  std::vector<int64_t> remove_keys = {1, 2, 3};
  for (auto key : remove_keys) {
    index_key.SetFromInteger(key);
    tree.Remove(index_key, transaction);
  }
  index_key.SetFromInteger(15);
  tree.Remove(index_key, transaction);

  int64_t size = 0;
  bool is_present;

  for (auto key : keys) {
    rids.clear();
    index_key.SetFromInteger(key);
    is_present = tree.GetValue(index_key, &rids);

    if (!is_present) {
      EXPECT_NE(std::find(remove_keys.begin(), remove_keys.end(), key), remove_keys.end());
    } else {
      EXPECT_EQ(rids.size(), 1);
      EXPECT_EQ(rids[0].GetPageId(), 0);
      EXPECT_EQ(rids[0].GetSlotNum(), key);
      size = size + 1;
    }
  }

  EXPECT_EQ(size, 1);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete bpm;
}
}  // namespace bustub
