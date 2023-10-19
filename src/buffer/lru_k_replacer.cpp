//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <cstddef>
#include <iterator>
#include <utility>
#include "common/config.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  // latch_.lock();
  std::lock_guard<std::mutex> lock(latch_);
  if (curr_size_ < 1) {
    return false;
  }
  frame_id_t temp_t;
  if (!below_.empty()) {  // 选择 below  中最小的删除
    temp_t = *below_.begin();
    below_k_.erase(temp_t);
    below_.pop_front();
  } else {  // 选择 over  中 最大的删除
    temp_t = *over_.begin();
    over_k_.erase(temp_t);
    over_.pop_front();
  }
  node_store_.erase(temp_t);

  *frame_id = temp_t;
  curr_size_--;

  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  if (frame_id < 0 || frame_id >= static_cast<int>(replacer_size_)) {
    exit(-1);  // 帧的大小 小于0 或者 大于了 替换器大小，无法访问。
  }
  std::lock_guard<std::mutex> lock(latch_);
  if (node_store_.find(frame_id) != node_store_.end()) {
    node_store_[frame_id].history_.push_back(current_timestamp_);
    if (node_store_[frame_id].is_evictable_) {
      // current frame is evictable
      size_t dis;
      if (node_store_[frame_id].k_ < k_) {
        node_store_[frame_id].k_++;
        if (node_store_[frame_id].k_ == k_) {
          // node_store_[frame_id].history_.begin()
          dis = below_k_[frame_id];
          std::pair<frame_id_t, size_t> new_temp = std::make_pair(frame_id, dis);
          below_k_.erase(frame_id);
          below_.remove(frame_id);  // 删掉，纯净一些
          auto it = over_.begin();
          while (it != over_.end()) {
            if (over_k_.find(*it)->second > new_temp.second) {
              over_.insert(it, frame_id);
              break;
            }
            it++;
          }
          if (it == over_.end()) {
            over_.push_back(frame_id);
          }
          over_k_.insert(new_temp);
        }
      } else if (node_store_[frame_id].k_ == k_) {
        node_store_[frame_id].history_.pop_front();
        dis = static_cast<size_t>(*node_store_[frame_id].history_.begin());
        std::pair<frame_id_t, size_t> new_temp = std::make_pair(frame_id, dis);
        over_.remove(frame_id);
        over_k_.erase(frame_id);
        auto it = over_.begin();
        while (it != over_.end()) {
          if (over_k_.find(*it)->second > new_temp.second) {
            over_.insert(it, frame_id);
            break;
          }
          it++;
        }
        if (it == over_.end()) {
          over_.push_back(frame_id);
        }
        over_k_.insert(new_temp);
      }

    } else {
      if (node_store_[frame_id].k_ < k_) {
        node_store_[frame_id].k_++;
      } else if (node_store_[frame_id].k_ == k_) {
        node_store_[frame_id].history_.pop_front();
      }
    }
  } else {
    auto *node = new LRUKNode();
    node->history_.push_back(current_timestamp_);
    node->k_++;
    node->fid_ = frame_id;
    // node_store_[frame_id]=*node;
    node_store_.insert(std::make_pair(frame_id, *node));
    delete node;
  }
  current_timestamp_++;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  if (frame_id < 0 || frame_id >= static_cast<int>(replacer_size_)) {
    exit(-1);  // 帧的大小 小于0 或者 大于了 替换器大小，无法访问。
  }
  std::lock_guard<std::mutex> lock(latch_);
  if (node_store_.find(frame_id) == node_store_.end()) {
    // node_store_ don't include such frameid
    exit(-1);
  }
  if (node_store_[frame_id].is_evictable_ ^ set_evictable) {
    node_store_[frame_id].is_evictable_ = set_evictable;
    if (set_evictable) {
      curr_size_++;
      if (node_store_[frame_id].k_ < k_) {
        // insert into below_k_
        std::pair<frame_id_t, size_t> new_temp =
            std::make_pair(frame_id, static_cast<size_t>(*node_store_[frame_id].history_.begin()));
        auto it = below_.begin();
        while (it != below_.end()) {
          if (below_k_.find(*it)->second > new_temp.second) {
            below_.insert(it, frame_id);
            break;
          }
          it++;
        }
        if (it == below_.end()) {
          below_.push_back(frame_id);
        }
        below_k_.insert(new_temp);
      } else {
        // insert into over_k_
        std::pair<frame_id_t, size_t> new_temp =
            std::make_pair(frame_id, static_cast<size_t>(*node_store_[frame_id].history_.begin()));
        auto it = over_.begin();
        while (it != over_.end()) {
          if (over_k_.find(*it)->second > new_temp.second) {
            over_.insert(it, frame_id);
            break;
          }
          it++;
        }
        if (it == over_.end()) {
          over_.push_back(frame_id);
        }
        over_k_.insert(new_temp);
      }
    } else {
      curr_size_--;
      if (node_store_[frame_id].k_ < k_) {
        below_k_.erase(frame_id);
        below_.remove(frame_id);
      } else {
        over_k_.erase(frame_id);
        over_.remove(frame_id);
      }
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  if (frame_id < 0 || frame_id >= static_cast<int>(replacer_size_)) {
    exit(-1);  // 帧的大小 小于0 或者 大于了 替换器大小，无法访问。
  }
  std::lock_guard<std::mutex> lock(latch_);
  if (node_store_.find(frame_id) == node_store_.end()) {
    // this frame_id is non-evictable;
    return;
  }
  if (!node_store_.find(frame_id)->second.is_evictable_) {
    // this frame_id is non-evictable;
    exit(-1);
  }
  if (node_store_[frame_id].k_ < k_) {
    below_k_.erase(frame_id);
    below_.remove(frame_id);
  } else {
    over_k_.erase(frame_id);
    over_.remove(frame_id);
  }
  node_store_.erase(frame_id);

  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);
  return curr_size_;
}
auto LRUKReplacer::MyCmp(std::pair<frame_id_t, size_t> elem1, std::pair<frame_id_t, size_t> elem2) -> bool {
  return elem1.second >= elem2.second;
}

void LRUKReplacer::Eroll(frame_id_t frame_id) {
  auto cont = node_store_[frame_id].history_;
  for (auto &it : cont) {
    std::cout << static_cast<size_t>(it) << std::endl;
  }
  // for (auto &it : over_) {
  //   std::cout << it;
  // }
  // std::cout << std::endl;
}
}  // namespace bustub
