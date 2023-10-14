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
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  //理解 ：让 缓冲池 多管理一个页面
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
  }
  *page_id = AllocatePage();
  // TOOD(unknown): writeback
  if (pages_[id].IsDirty()) {
    disk_manager_->WritePage(pages_[id].GetPageId(), pages_[id].GetData());
  }
  // TODO(unknown): 清理page内容；
  pages_[id].ResetMemory();
  pages_[id].page_id_ = *page_id;
  pages_[id].pin_count_++;
  disk_manager_->ReadPage(*page_id, pages_[id].data_);
  replacer_->RecordAccess(id);
  replacer_->SetEvictable(id, false);
  // emplace  不会删除这个页面。
  page_table_.emplace(std::make_pair(*page_id, id));
  return &pages_[id];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  //设想进程请求页面
  // for (size_t i = 0; i < pool_size_; ++i) {
  //   if(pages_[i].GetPageId()==page_id){
  //     replacer_->RecordAccess(i);
  //     return &pages_[i];
  //   }
  // }
  // 优化： 用 find；
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    replacer_->RecordAccess(it->second);
    // replacer_->SetEvictable(it->second, false);
    pages_[it->second].pin_count_++;
    return &pages_[it->second];
  }
  frame_id_t id;
  if (!free_list_.empty()) {
    id = static_cast<int>(*free_list_.begin());
    free_list_.pop_front();
    pages_[id].page_id_ = page_id;

  } else if (!replacer_->Evict(&id)) {
    return nullptr;
  } else {
    if (pages_[id].IsDirty()) {
      disk_manager_->WritePage(pages_[id].GetPageId(), pages_[id].GetData());
    }
    pages_[id].ResetMemory();
    pages_[id].page_id_ = page_id;
  }
  disk_manager_->ReadPage(page_id, pages_[id].data_);
  pages_[id].pin_count_++;
  replacer_->RecordAccess(id);
  replacer_->SetEvictable(id, false);
  page_table_.emplace(std::make_pair(page_id, id));
  return &pages_[id];
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  // for (size_t i = 0; i < pool_size_; ++i) {
  //   if(pages_[i].GetPageId()==page_id){
  //     if(pages_[i].GetPinCount()>0){
  //       pages_[i].pin_count_=0;
  //       replacer_->SetEvictable(i, true);
  //       pages_[i].is_dirty_=is_dirty;
  //       return true;
  //     }
  //   }
  // }
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    pages_[it->second].pin_count_ = 0;
    pages_[it->second].is_dirty_ = is_dirty;
    replacer_->SetEvictable(it->second, true);
    return true;
  }
  //优化，使用 page_table_

  //设想是一个进程结束调用了它
  return false;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  // for (size_t i = 0; i < pool_size_; ++i) {
  //   if(pages_[i].GetPageId()==page_id){
  //     disk_manager_->WritePage( page_id, pages_[i].GetData());
  //     pages_[i].is_dirty_=false;
  //     return true;
  //   }
  // }
  // 优化，使用page_table_;
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
  // for (size_t i = 0; i < pool_size_; ++i) {
  //   if(pages_[i].GetPageId()==page_id){
  //     if(pages_[i].GetPinCount()>0){
  //       return false;
  //     }
  //     if(pages_[i].IsDirty()){
  //       disk_manager_->WritePage(pages_[i].GetPageId(), pages_[i].GetData());
  //     }
  //     pages_[i].ResetMemory();
  //     replacer_->Remove( static_cast<frame_id_t>(i));
  //     free_list_.push_back(i);
  //     return true;
  //   }
  // }
  // 优化，使用page_table_-----因为 无法判断是否在 pool中
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
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    replacer_->RecordAccess(it->second);
    // replacer_->SetEvictable(it->second, false);
    pages_[it->second].pin_count_++;
    return {this, &pages_[it->second]};
  }
  frame_id_t id;
  if (!free_list_.empty()) {
    id = static_cast<int>(*free_list_.begin());
    free_list_.pop_front();
    pages_[id].page_id_ = page_id;

  } else if (!replacer_->Evict(&id)) {
    return {this, nullptr};
  } else {
    if (pages_[id].IsDirty()) {
      disk_manager_->WritePage(pages_[id].GetPageId(), pages_[id].GetData());
    }
    pages_[id].ResetMemory();
    pages_[id].page_id_ = page_id;
  }
  disk_manager_->ReadPage(page_id, pages_[id].data_);
  pages_[id].pin_count_++;
  replacer_->RecordAccess(id);
  replacer_->SetEvictable(id, false);
  page_table_.emplace(std::make_pair(page_id, id));
  return {this, &pages_[id]};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    replacer_->RecordAccess(it->second);
    // replacer_->SetEvictable(it->second, false);
    pages_[it->second].pin_count_++;
    return {this, &pages_[it->second]};
  }
  frame_id_t id;
  if (!free_list_.empty()) {
    id = static_cast<int>(*free_list_.begin());
    free_list_.pop_front();
    pages_[id].page_id_ = page_id;

  } else if (!replacer_->Evict(&id)) {
    return {this, nullptr};
  } else {
    if (pages_[id].IsDirty()) {
      disk_manager_->WritePage(pages_[id].GetPageId(), pages_[id].GetData());
    }
    pages_[id].ResetMemory();
    pages_[id].page_id_ = page_id;
  }
  disk_manager_->ReadPage(page_id, pages_[id].data_);
  pages_[id].pin_count_++;
  replacer_->RecordAccess(id);
  replacer_->SetEvictable(id, false);
  page_table_.emplace(std::make_pair(page_id, id));
  return {this, &pages_[id]};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    replacer_->RecordAccess(it->second);
    // replacer_->SetEvictable(it->second, false);
    pages_[it->second].pin_count_++;
    pages_[it->second].is_dirty_ = true;
    return {this, &pages_[it->second]};
  }
  frame_id_t id;
  if (!free_list_.empty()) {
    id = static_cast<int>(*free_list_.begin());
    free_list_.pop_front();
    pages_[id].page_id_ = page_id;

  } else if (!replacer_->Evict(&id)) {
    return {this, nullptr};
  } else {
    if (pages_[id].IsDirty()) {
      disk_manager_->WritePage(pages_[id].GetPageId(), pages_[id].GetData());
    }
    pages_[id].ResetMemory();
    pages_[id].page_id_ = page_id;
  }
  disk_manager_->ReadPage(page_id, pages_[id].data_);
  pages_[id].pin_count_++;
  pages_[id].is_dirty_ = true;
  replacer_->RecordAccess(id);
  replacer_->SetEvictable(id, false);
  page_table_.emplace(std::make_pair(page_id, id));
  return {this, &pages_[id]};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  //理解 ：让 缓冲池 多管理一个页面
  if (replacer_->Size() == 0 && free_list_.empty()) {
    return {this, nullptr};
  }

  frame_id_t id;
  if (!free_list_.empty()) {
    id = static_cast<int>(*free_list_.begin());
    free_list_.pop_front();
  } else if (!replacer_->Evict(&id)) {
    return {this, nullptr};
  } else {
    page_table_.erase(pages_[id].GetPageId());
  }
  *page_id = AllocatePage();
  // TOOD(unknown): writeback
  if (pages_[id].IsDirty()) {
    disk_manager_->WritePage(pages_[id].GetPageId(), pages_[id].GetData());
  }
  // TODO(unknown): 清理page内容；
  pages_[id].ResetMemory();
  pages_[id].page_id_ = *page_id;
  pages_[id].pin_count_++;
  disk_manager_->ReadPage(*page_id, pages_[id].data_);
  replacer_->RecordAccess(id);
  replacer_->SetEvictable(id, false);
  // emplace  不会删除这个页面。
  page_table_.emplace(std::make_pair(*page_id, id));

  return {this, &pages_[id]};
}

}  // namespace bustub
