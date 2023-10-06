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
#include "common/exception.h"

namespace bustub {

LRUKNode::LRUKNode(frame_id_t fid_):fid_(fid_){}
//这里我就直接假设 页框  1，2，3，4
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
    // all frames are initially considered non-evictable
    evictable_=new bool[num_frames];
    for(int i=0;i<num_frames;i++){
        evictable_[i]=0;
    }
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {

    return false; 
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
    if(frame_id<0||frame_id>=replacer_size_){
        exit(-1);//帧的大小 小于0 或者 大于了 替换器大小，无法访问。
    }
    //store_node
    if(node_store_.find(frame_id)!=node_store_.end()){
        node_store_[frame_id].history_.push_back(current_timestamp_);
            if(node_store_[frame_id].k_<k_)
                 node_store_[frame_id].k_++;
             else if(node_store_[frame_id].k_=k_){
                 node_store_[frame_id].history_.pop_front();
             }
    }else{
        LRUKNode* node =new LRUKNode(frame_id);
        node->history_.push_back(current_timestamp_);
        node->k_++;
        node_store_[frame_id]=*node;
    }
    current_timestamp_++;
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    if(frame_id<0||frame_id>=replacer_size_){
        exit(-1);//帧的大小 小于0 或者 大于了 替换器大小，无法访问。
    }
    if(evictable_[frame_id]^set_evictable){
        evictable_[frame_id]=set_evictable;
        if(set_evictable){
            curr_size_++;
        }else{
            curr_size_--;
        }
    }

}

void LRUKReplacer::Remove(frame_id_t frame_id) { 

    node_store_.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t { 


    return curr_size_; 

}

}  // namespace bustub