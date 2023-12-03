
#include <algorithm>
#include <cstdio>
#include <random>

#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/b_plus_tree_page.h"
#include "test_util.h"  // NOLINT

namespace bustub {

using bustub::DiskManagerUnlimitedMemory;

// helper function to launch multiple threads
template <typename... Args>
void LaunchParallelTest(uint64_t num_threads, Args &&...args) {
  std::vector<std::thread> thread_group;

  // Launch a group of threads
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group.push_back(std::thread(args..., thread_itr));
  }

  // Join the threads with the main thread
  for (uint64_t thread_itr = 0; thread_itr < num_threads; ++thread_itr) {
    thread_group[thread_itr].join();
  }
}

// helper function to insert
void InsertHelper(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                  __attribute__((unused)) uint64_t thread_itr = 0) {
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    // std::cout << tree->DrawBPlusTree() << std::endl;
    tree->Insert(index_key, rid, transaction);
  }
  delete transaction;
}

// helper function to seperate insert
void InsertHelperSplit(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                       int total_threads, __attribute__((unused)) uint64_t thread_itr) {
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);
  for (auto key : keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      int64_t value = key & 0xFFFFFFFF;
      rid.Set(static_cast<int32_t>(key >> 32), value);
      index_key.SetFromInteger(key);
      tree->Insert(index_key, rid, transaction);
    }
  }
  delete transaction;
}

// helper function to delete
void DeleteHelper(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &remove_keys,
                  __attribute__((unused)) uint64_t thread_itr = 0) {
  GenericKey<8> index_key;
  // create transaction
  auto *transaction = new Transaction(0);
  for (auto key : remove_keys) {
    // int64_t value = key & 0xFFFFFFFF;
    // if (value % 5 == 0) {
    //   std::cout << "hah" << std::endl;
    // }
    // std::cout << tree->DrawBPlusTree() << std::endl;
    index_key.SetFromInteger(key);
    tree->Remove(index_key, transaction);
  }
  delete transaction;
}

// helper function to seperate delete
void DeleteHelperSplit(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree,
                       const std::vector<int64_t> &remove_keys, int total_threads,
                       __attribute__((unused)) uint64_t thread_itr) {
  GenericKey<8> index_key;
  // create transaction
  auto *transaction = new Transaction(0);
  for (auto key : remove_keys) {
    if (static_cast<uint64_t>(key) % total_threads == thread_itr) {
      index_key.SetFromInteger(key);
      tree->Remove(index_key, transaction);
    }
  }
  delete transaction;
}

void LookupHelper(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                  uint64_t tid, __attribute__((unused)) uint64_t thread_itr = 0) {
  auto *transaction = new Transaction(static_cast<txn_id_t>(tid));
  GenericKey<8> index_key;
  RID rid;
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    std::vector<RID> result;
    bool res = tree->GetValue(index_key, &result, transaction);
    ASSERT_EQ(res, true);
    ASSERT_EQ(result.size(), 1);
    ASSERT_EQ(result[0], rid);
  }
  delete transaction;
}
TEST(BPlusTreeConcurrentTest, MyMixTest) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());

  // create and fetch header_page
  page_id_t page_id;
  auto *header_page = bpm->NewPage(&page_id);
  (void)header_page;

  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator);

  // Add perserved_keys
  std::vector<int64_t> perserved_keys;
  std::vector<int64_t> dynamic_keys;
  int64_t total_keys = 20000;
  int64_t sieve = 5;
  for (int64_t i = 1; i <= total_keys; i++) {
    if (i % sieve == 0) {
      perserved_keys.push_back(i);
    } else {
      dynamic_keys.push_back(i);
    }
  }
  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(perserved_keys.begin(), perserved_keys.end(), g);
  std::shuffle(dynamic_keys.begin(), dynamic_keys.end(), g);
  InsertHelper(&tree, perserved_keys, 1);

  // InsertHelper(&tree, dynamic_keys, 1);
  // LookupHelper(&tree, perserved_keys, 1);
  // DeleteHelper(&tree, dynamic_keys, 1);
  // InsertHelper(&tree, perserved_keys, 1);
  // InsertHelper(&tree, dynamic_keys, 1);
  // LookupHelper(&tree, perserved_keys, 1);
  // DeleteHelper(&tree, dynamic_keys, 1);
  // LookupHelper(&tree, perserved_keys, 1);

  // Check there are 1000 keys in there
  size_t size = 0;
  for (auto iter = tree.Begin(); iter != tree.End(); ++iter) {
    const auto &pair = *iter;
    if ((pair.first).ToString() % sieve == 0) {
      size++;
    }
  }

  ASSERT_EQ(size, perserved_keys.size());
  auto insert_task = [&](int tid) { InsertHelper(&tree, dynamic_keys, tid); };
  auto delete_task = [&](int tid) { DeleteHelper(&tree, dynamic_keys, tid); };
  auto lookup_task = [&](int tid) { LookupHelper(&tree, perserved_keys, tid); };
  auto iterate_task = [&](int tid) {
    for (int i = 0; i < 10; ++i) {
      int start_key = perserved_keys[i];
      GenericKey<8> start_key_generic;
      start_key_generic.SetFromInteger(start_key);
      size_t size_inernal = i;
      for (auto iter = tree.Begin(start_key_generic); iter != tree.End(); ++iter) {
        const auto &pair = *iter;
        if ((pair.first).ToString() % sieve == 0) {
          // EXPECT_EQ((pair.first).ToString(), perserved_keys[size_inernal]);
          size_inernal++;
        }
      }
    }
  };
  (void)iterate_task;
  // std::thread thread1{delete_task, 1};
  // std::thread thread2{delete_task, 2};
  // std::thread thread3{insert_task, 3};
  // std::thread thread4{insert_task, 4};
  // std::thread thread8{delete_task, 1};
  // std::thread thread5{delete_task, 2};
  // std::thread thread6{insert_task, 3};
  // std::thread thread7{insert_task, 4};
  // thread1.join();
  // thread2.join();
  // thread3.join();
  // thread4.join();
  // thread5.join();
  // thread6.join();
  // thread8.join();
  // thread7.join();
  // LookupHelper(&tree, perserved_keys, 11);
  std::vector<std::thread> threads;
  std::vector<std::function<void(int)>> tasks;
  tasks.emplace_back(insert_task);
  tasks.emplace_back(delete_task);
  tasks.emplace_back(lookup_task);
  tasks.emplace_back(iterate_task);

  size_t num_threads = 16;
  for (size_t i = 0; i < num_threads; i++) {
    threads.emplace_back(tasks[i % tasks.size()], i);
  }
  for (size_t i = 0; i < num_threads; i++) {
    threads[i].join();
  }

  // Check all reserved keys exist
  size = 0;

  for (auto iter = tree.Begin(); iter != tree.End(); ++iter) {
    const auto &pair = *iter;
    if ((pair.first).ToString() % sieve == 0) {
      size++;
    }
  }

  ASSERT_EQ(size, perserved_keys.size());

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete bpm;
}

TEST(BPlusTreeConcurrentTest, MyMixTestIterator) {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto *bpm = new BufferPoolManager(50, disk_manager.get());

  // create and fetch header_page
  page_id_t page_id;
  auto *header_page = bpm->NewPage(&page_id);
  (void)header_page;

  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", page_id, bpm, comparator, 3, 5);

  // Add perserved_keys
  std::vector<int64_t> perserved_keys;
  std::vector<int64_t> dynamic_keys;
  int64_t total_keys = 20;
  int64_t sieve = 2;
  for (int64_t i = 1; i <= total_keys; i++) {
    if (i % sieve == 0) {
      perserved_keys.push_back(i);
    } else {
      dynamic_keys.push_back(i);
    }
  }
  // std::random_device rd;
  // std::mt19937 g(rd());
  // std::shuffle(perserved_keys.begin(), perserved_keys.end(), g);
  // std::shuffle(dynamic_keys.begin(), dynamic_keys.end(), g);
  InsertHelper(&tree, dynamic_keys, 1);

  auto insert_task = [&](int tid) { InsertHelper(&tree, perserved_keys, tid); };
  auto delete_task = [&](int tid) { DeleteHelper(&tree, dynamic_keys, tid); };
  auto iterate_task = [&](int tid) {
    for (int i = 0; i < 10; ++i) {
      int start_key = perserved_keys[i];
      GenericKey<8> start_key_generic;
      start_key_generic.SetFromInteger(start_key);
      size_t size_inernal = i;
      for (auto iter = tree.Begin(start_key_generic); iter != tree.End(); ++iter) {
        const auto &pair = *iter;
        if ((pair.first).ToString() % sieve == 0) {
          EXPECT_EQ((pair.first).ToString(), perserved_keys[size_inernal]);
          size_inernal++;
        }
      }
    }
  };

  std::vector<std::thread> threads;
  std::vector<std::function<void(int)>> tasks;
  tasks.emplace_back(insert_task);
  tasks.emplace_back(delete_task);
  // tasks.emplace_back(iterate_task);

  size_t num_threads = 8;
  for (size_t i = 0; i < num_threads; i++) {
    threads.emplace_back(tasks[i % tasks.size()], i);
  }
  for (size_t i = 0; i < num_threads; i++) {
    threads[i].join();
  }
  for (auto iter = tree.Begin(); iter != tree.End(); ++iter) {
    const auto &pair = *iter;
    std::cout << pair.first << std::endl;
  }
  for (int i = 0; i < 10; ++i) {
    int start_key = perserved_keys[i];
    GenericKey<8> start_key_generic;
    start_key_generic.SetFromInteger(start_key);
    size_t size_inernal = i;
    for (auto iter = tree.Begin(start_key_generic); iter != tree.End(); ++iter) {
      const auto &pair = *iter;
      std::cout << pair.first << std::endl;
      if ((pair.first).ToString() % sieve == 0) {
        EXPECT_EQ((pair.first).ToString(), perserved_keys[size_inernal]);
        size_inernal++;
      }
    }
  }
  threads.clear();
  for (size_t i = 0; i < num_threads; i++) {
    threads.emplace_back(iterate_task, i);
  }
  for (size_t i = 0; i < num_threads; i++) {
    threads[i].join();
  }

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete bpm;
}
}  // namespace bustub
