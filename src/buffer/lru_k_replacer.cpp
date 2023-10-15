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
#include <utility>
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  std::cout << "==========LRUKReplacer======"
            << "---replacer_size_:" << num_frames << " ----- "
            << "k_:" << k_ << " --------- " << std::endl;
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::cout << "==IN========Evict======"
            << "---frame_id:" << *frame_id << " ----- --------- " << std::endl;
  if (curr_size_ < 1) {
    return false;
  }
  frame_id_t temp_t;
  for (auto &it : below_k_) {
    frame_id_t key = it.first;
    key++;
    size_t value = it.second;
    value++;
  }
  if (!below_k_.empty()) {
    auto it = below_k_.begin();
    auto previt = below_k_.begin();

    // 在容器中遍历，找到倒数第二个元素的迭代器
    while (it != below_k_.end()) {
      previt = it;
      ++it;
    }

    temp_t = previt->first;  // (below_k_.begin()->first);
    below_k_.erase(temp_t);
  } else {
    auto it = over_k_.begin();
    auto previt = over_k_.begin();

    // 在容器中遍历，找到倒数第二个元素的迭代器
    while (it != over_k_.end()) {
      previt = it;
      ++it;
    }
    temp_t = previt->first;
    over_k_.erase(temp_t);
  }
  node_store_.erase(temp_t);

  *frame_id = temp_t;
  curr_size_--;
  std::cout << "==OUT========LRUKReplacer======"
            << "---replacer_size_:" << *frame_id << " ----- "
            << "Size():" << Size() << " --------- " << std::endl;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::cout << "==IN========RecordAccess======"
            << "---frame_id:" << frame_id << " ----- "
            << "Size():" << Size() << " --------- " << std::endl;

  if (frame_id < 0 || frame_id >= static_cast<int>(replacer_size_)) {
    exit(-1);  // 帧的大小 小于0 或者 大于了 替换器大小，无法访问。
  }
  // store_node---
  if (node_store_.find(frame_id) != node_store_.end()) {
    node_store_[frame_id].history_.push_back(current_timestamp_);
    if (node_store_[frame_id].is_evictable_) {
      // current frame is evictable
      if (node_store_[frame_id].k_ < k_) {
        node_store_[frame_id].k_++;
        if (node_store_[frame_id].k_ == k_) {
          // node_store_[frame_id].history_.begin()
          size_t dis = current_timestamp_ - below_k_[frame_id];
          std::pair<frame_id_t, size_t> new_temp = std::make_pair(frame_id, dis);

          below_k_.erase(frame_id);
          auto it = std::upper_bound(over_k_.begin(), over_k_.end(), new_temp, LRUKReplacer::MyCmp);
          it = over_k_.insert(it, new_temp);
        }
      } else if (node_store_[frame_id].k_ == k_) {
        node_store_[frame_id].history_.pop_front();
        // update
        over_k_[frame_id] = current_timestamp_ - static_cast<size_t>(*node_store_[frame_id].history_.begin());
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
  std::cout << "==OUT========RecordAccess======"
            << "---frame_id:" << frame_id << " ----- "
            << "Size():" << Size() << " --------- " << std::endl;
  current_timestamp_++;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::cout << "==IN========SetEvictable======"
            << "---frame_id:" << frame_id << " ----- "
            << "set_evictable:" << set_evictable << " --------- " << std::endl;
  if (frame_id < 0 || frame_id >= static_cast<int>(replacer_size_)) {
    exit(-1);  // 帧的大小 小于0 或者 大于了 替换器大小，无法访问。
  }
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
        auto it = std::lower_bound(below_k_.begin(), below_k_.end(), new_temp, LRUKReplacer::MyCmp);
        it = below_k_.insert(it, new_temp);

      } else {
        // insert into over_k_
        std::pair<frame_id_t, size_t> new_temp =
            std::make_pair(frame_id, static_cast<size_t>(*node_store_[frame_id].history_.rbegin()) -
                                         static_cast<size_t>(*node_store_[frame_id].history_.begin()));
        auto it = std::upper_bound(over_k_.begin(), over_k_.end(), new_temp, LRUKReplacer::MyCmp);
        it = over_k_.insert(it, new_temp);
      }
    } else {
      curr_size_--;
      if (node_store_[frame_id].k_ < k_) {
        below_k_.erase(frame_id);
      } else {
        over_k_.erase(frame_id);
      }
    }
  }
  std::cout << "==OUT========SetEvictable======"
            << "---frame_id:" << frame_id << " ----- "
            << "Size:" << Size() << " --------- " << std::endl;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::cout << "==IN========Remove======"
            << "---frame_id:" << frame_id << " ----- "
            << "Size:" << Size() << " --------- " << std::endl;
  if (frame_id < 0 || frame_id >= static_cast<int>(replacer_size_)) {
    exit(-1);  // 帧的大小 小于0 或者 大于了 替换器大小，无法访问。
  }
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
  } else {
    over_k_.erase(frame_id);
  }
  node_store_.erase(frame_id);

  curr_size_--;
  std::cout << "==OUT========Remove======"
            << "---frame_id:" << frame_id << " ----- "
            << "Size:" << Size() << " --------- " << std::endl;
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }
auto LRUKReplacer::MyCmp(std::pair<frame_id_t, size_t> elem1, std::pair<frame_id_t, size_t> elem2) -> bool {
  return elem1.second >= elem2.second;
}
}  // namespace bustub
