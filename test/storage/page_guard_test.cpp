//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard_test.cpp
//
// Identification: test/storage/page_guard_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdio>
#include <random>
#include <string>

#include "buffer/buffer_pool_manager.h"
#include "storage/disk/disk_manager_memory.h"
#include "storage/page/page_guard.h"

#include "gtest/gtest.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(PageGuardTest, DISABLED_SampleTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp;
  {
    auto *page0 = bpm->NewPage(&page_id_temp);
    // page0->RUnlatch();
    // page0->RUnlatch();
    // page0->RLatch();
    // page0->RUnlatch();
    // page0->WUnlatch();
    // page0->WUnlatch();
    // page0->WUnlatch();
    // page0->WLatch();
    auto guarded_page = BasicPageGuard(bpm.get(), page0);
  }
  {
    auto *page1 = bpm->NewPage(&page_id_temp);
    auto *page2 = bpm->NewPage(&page_id_temp);

    auto guarded_page1 = ReadPageGuard(bpm.get(), page1);
    auto guarded_page11 = BasicPageGuard(bpm.get(), page1);

    auto guarded_page2 = BasicPageGuard(bpm.get(), page2);
    auto guarded_page22 = WritePageGuard(bpm.get(), page2);
    guarded_page1.Drop();
    guarded_page22.Drop();

    guarded_page22.Drop();
  }
  {
    auto *page3 = bpm->NewPage(&page_id_temp);
    auto *page4 = bpm->NewPage(&page_id_temp);
    auto guarded_page1 = WritePageGuard(bpm.get(), page3);
    auto guarded_page2 = WritePageGuard(bpm.get(), page4);
    guarded_page1 = std::move(guarded_page2);
    auto guarded_page3 = WritePageGuard(std::move(guarded_page2));
    guarded_page2.PageId();
  }

  // EXPECT_EQ(page0->GetData(), guarded_page.GetData());
  // EXPECT_EQ(page0->GetPageId(), guarded_page.PageId());
  // EXPECT_EQ(1, page0->GetPinCount());

  // guarded_page.Drop();

  // EXPECT_EQ(0, page0->GetPinCount());

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
}

TEST(PageGuardTest, HHTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 5;
  const size_t k = 2;

  auto disk_manager = std::make_shared<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get(), k);

  page_id_t page_id_temp = 0;
  page_id_t page_id_temp_a;
  auto *page0 = bpm->NewPage(&page_id_temp);
  auto *page1 = bpm->NewPage(&page_id_temp_a);

  auto guarded_page = BasicPageGuard(bpm.get(), page0);
  auto guarded_page_a = BasicPageGuard(bpm.get(), page1);

  // after drop, whether destructor decrements the pin_count_ ?
  {
    auto read_guard1 = bpm->FetchPageRead(page_id_temp_a);
    EXPECT_EQ(2, page1->GetPinCount());
    read_guard1.Drop();
    EXPECT_EQ(1, page1->GetPinCount());
  }
  EXPECT_EQ(1, page0->GetPinCount());
  EXPECT_EQ(1, page1->GetPinCount());
  // test the move assignment
  {
    auto read_guard1 = bpm->FetchPageRead(page_id_temp);
    auto read_guard2 = bpm->FetchPageRead(page_id_temp_a);
    EXPECT_EQ(2, page0->GetPinCount());
    EXPECT_EQ(2, page1->GetPinCount());
    read_guard2 = std::move(read_guard1);
    EXPECT_EQ(2, page0->GetPinCount());
    EXPECT_EQ(1, page1->GetPinCount());
  }
  EXPECT_EQ(1, page0->GetPinCount());
  // test the move constructor
  {
    auto read_guard1 = bpm->FetchPageRead(page_id_temp);
    auto read_guard2(std::move(read_guard1));
    auto read_guard3(std::move(read_guard2));
    EXPECT_EQ(2, page0->GetPinCount());
  }
  EXPECT_EQ(1, page0->GetPinCount());
  EXPECT_EQ(page_id_temp, page0->GetPageId());

  // repeat drop
  guarded_page.Drop();
  EXPECT_EQ(0, page0->GetPinCount());
  guarded_page.Drop();
  EXPECT_EQ(0, page0->GetPinCount());

  disk_manager->ShutDown();
}

}  // namespace bustub
