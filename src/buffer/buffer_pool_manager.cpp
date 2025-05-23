//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include <cstddef>
#include <cstdlib>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

  // we allocate a consecutive memory space for the buffer pool
  std::cout << "pool_size" << pool_size << "  replacer_k" << replacer_k << std::endl;
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  // 理解 ：让 缓冲池 多管理一个页面
  std::lock_guard<std::mutex> lock(latch_);
  *page_id = INVALID_PAGE_ID;
  if (replacer_->Size() == 0 && free_list_.empty()) {
    return nullptr;
  }

  frame_id_t id;

  if (!free_list_.empty()) {
    id = static_cast<int>(*free_list_.begin());
    free_list_.pop_front();

  } else if (!replacer_->Evict(&id)) {
    return nullptr;
  } else {
    page_table_.erase(pages_[id].GetPageId());
    // TOOD(unknown): writeback
    if (pages_[id].IsDirty()) {
      disk_manager_->WritePage(pages_[id].GetPageId(), pages_[id].GetData());
    }

    // DeallocatePage(pages_[id].GetPageId());
  }
  *page_id = AllocatePage();
  pages_[id].ResetMemory();
  pages_[id].pin_count_++;
  pages_[id].page_id_ = *page_id;
  pages_[id].is_dirty_ = false;
  disk_manager_->ReadPage(*page_id, pages_[id].data_);
  replacer_->RecordAccess(id);
  replacer_->SetEvictable(id, false);
  // emplace  不会删除这个页面。
  page_table_.emplace(std::make_pair(*page_id, id));
  return &pages_[id];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    replacer_->RecordAccess(it->second);
    replacer_->SetEvictable(it->second, false);
    pages_[it->second].pin_count_++;
    // pages_[it->second].is_dirty_ = true;
    return &pages_[it->second];
  }
  frame_id_t id;
  if (!free_list_.empty()) {
    id = static_cast<int>(*free_list_.begin());
    free_list_.pop_front();

  } else if (!replacer_->Evict(&id)) {
    return nullptr;
  } else {
    page_table_.erase(pages_[id].GetPageId());
    if (pages_[id].IsDirty()) {
      disk_manager_->WritePage(pages_[id].GetPageId(), pages_[id].GetData());
    }
    // DeallocatePage(pages_[id].GetPageId());
  }
  pages_[id].ResetMemory();
  pages_[id].page_id_ = page_id;
  disk_manager_->ReadPage(page_id, pages_[id].data_);
  pages_[id].pin_count_++;
  pages_[id].is_dirty_ = false;
  replacer_->RecordAccess(id);
  replacer_->SetEvictable(id, false);
  page_table_.emplace(std::make_pair(page_id, id));
  return &pages_[id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    if (pages_[it->second].pin_count_ > 0) {
      pages_[it->second].pin_count_--;
      if (pages_[it->second].pin_count_ == 0) {
        replacer_->SetEvictable(it->second, true);
      }
    }
    if (!pages_[it->second].is_dirty_) {
      pages_[it->second].is_dirty_ = is_dirty;
    }
    return true;
  }
  return false;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    disk_manager_->WritePage(page_id, pages_[it->second].GetData());
    replacer_->RecordAccess(it->second);
    replacer_->SetEvictable(it->second, true);
    // pages_[it->second].pin_count_++;
    pages_[it->second].is_dirty_ = false;
    return true;
  }
  return false;
}

void BufferPoolManager::FlushAllPages() {
  for (auto &it : page_table_) {
    disk_manager_->WritePage(pages_[it.second].GetPageId(), pages_[it.second].GetData());
    replacer_->RecordAccess(it.second);
    pages_[it.second].is_dirty_ = false;
    replacer_->SetEvictable(it.second, true);
    // pages_[i].pin_count_++;
    // pages_[i].is_dirty_=false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return true;
  }
  if (pages_[it->second].GetPinCount() > 0) {
    return false;
  }
  if (pages_[it->second].IsDirty()) {
    disk_manager_->WritePage(page_id, pages_[it->second].GetData());
  }
  replacer_->Remove(it->second);
  free_list_.emplace_back(static_cast<int>(it->second));
  pages_[it->second].ResetMemory();
  // pages_[it->second].pin_count_++;
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  Page *page = FetchPage(page_id);
  return {this, page};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  Page *page = FetchPage(page_id);
  if (page != nullptr) {
    page->RLatch();
  }
  return {this, page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  // if (page_id < 0) {
  //   std::cout << "卧槽"
  //             << " 真有你的" << std::endl;
  // }
  Page *page = FetchPage(page_id);
  if (page != nullptr) {
    page->WLatch();
  }
  return {this, page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  Page *page = NewPage(page_id);
  return {this, page};
}

}  // namespace bustub
