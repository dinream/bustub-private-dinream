/**
 * lru_k_replacer_test.cpp
 */

#include "buffer/lru_k_replacer.h"

#include <algorithm>
#include <cstdio>
#include <memory>
#include <random>
#include <set>
#include <thread>  // NOLINT
#include <utility>
#include <vector>

#include "gtest/gtest.h"

namespace bustub {

TEST(LRUKReplacerTest, DISABLED_SampleTest) {
  std::map<frame_id_t, size_t> below_k;
  below_k.insert(std::make_pair(3, 9));
  below_k.insert(std::make_pair(5, 9));
  below_k.insert(below_k.find(5), std::make_pair(2, 9));
  for (auto &it : below_k) {
    std::cout << it.first << std::endl;
  }

  LRUKReplacer lru_replacer(10, 3);
  int value;
  // Scenario: add six elements to the replacer. We have [1,2,3,4,5]. Frame 6 is non-evictable.
  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(2);
  lru_replacer.RecordAccess(3);
  lru_replacer.RecordAccess(4);
  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(2);
  lru_replacer.RecordAccess(3);
  lru_replacer.RecordAccess(1);
  lru_replacer.RecordAccess(2);
  lru_replacer.Eroll(2);
  lru_replacer.SetEvictable(1, true);
  lru_replacer.Eroll(2);
  lru_replacer.SetEvictable(2, true);
  lru_replacer.Eroll(2);
  lru_replacer.SetEvictable(3, true);
  lru_replacer.SetEvictable(4, true);

  lru_replacer.Evict(&value);
  lru_replacer.RecordAccess(4);
  lru_replacer.RecordAccess(4);
  lru_replacer.Eroll(1);
  lru_replacer.Eroll(2);
  lru_replacer.Eroll(4);
  lru_replacer.Evict(&value);
  lru_replacer.Evict(&value);
  lru_replacer.Evict(&value);
  lru_replacer.Eroll(2);
}
}  // namespace bustub
