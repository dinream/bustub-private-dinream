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
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
    if(curr_size_<1)return false;
    if(below_k_.size()>0){
        *frame_id = (below_k_.begin()->first);
        below_k_.erase(*frame_id);
    }else{
        *frame_id=(std::prev(over_k_.end())->first);
        over_k_.erase(*frame_id);
    }
    node_store_.erase(*frame_id);
    return true; 
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
    if(frame_id<0||frame_id>=replacer_size_){
        exit(-1);//帧的大小 小于0 或者 大于了 替换器大小，无法访问。
    }
    //store_node---
    if(node_store_.find(frame_id)!=node_store_.end()){
        node_store_[frame_id].history_.push_back(current_timestamp_);
        if(node_store_[frame_id].is_evictable_){
            //current frame is evictable
            if(node_store_[frame_id].k_<k_){
                node_store_[frame_id].k_++;
                if(node_store_[frame_id].k_==k_){
                    //node_store_[frame_id].history_.begin()
                    size_t dis=below_k_[frame_id]-current_timestamp_;
                    std::pair<frame_id_t,size_t> newtemp=std::make_pair(frame_id,dis);
                    //TODO:insert it into over_k_   remove it from below_k_
                    below_k_.erase(frame_id);
                    auto it = std::upper_bound(over_k_.begin(),over_k_.end(),new_temp,mycmp);
                    it = over_k_.insert(it,new_temp);

                }
            }
            else if(node_store_[frame_id].k_==k_){
                node_store_[frame_id].history_.pop_front();
                //update
                over_k_[frame_id]=*(size_t *)&(node_store_[frame_id].history_.begin())-current_timestamp_;

            }
        }else{
            if(node_store_[frame_id].k_<k_){
                node_store_[frame_id].k_++;
            }else if(node_store_[frame_id].k_==k_){
                node_store_[frame_id].history_.pop_front();
            }
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
    if(node_store_.find(frame_id)==node_store_.end()){
        //node_store_ don't include such frameid
        exit(-1);
    }
    if(node_store_[frame_id].is_evictable_^set_evictable){
        node_store_[frame_id].is_evictable_=set_evictable;
        if(set_evictable){
            curr_size_++;
            if(node_store_[frame_id].k_<k_){
                //insert into below_k_
                std::pair<frame_id_t,size_t> new_temp=std::make_pair(frame_id,*(size_t*)&node_store_[frame_id].history_.begin());
                auto it = std::upper_bound(below_k_.begin(),below_k_.end(),new_temp, mycmp);
                it = below_k_.insert(it,new_temp);

            }
            else {
                //insert into over_k_
                std::pair<frame_id_t,size_t> new_temp=std::make_pair(frame_id,*(size_t*)&node_store_[frame_id].history_.rbegin()-*(size_t*)&node_store_[frame_id].history_.begin());
                auto it = std::upper_bound(over_k_.begin(),over_k_.end(),new_temp,mycmp);
                it = over_k_.insert(it,new_temp);
            }
        }else{
            curr_size_--;
            if(node_store_[frame_id].k_<k_){
                below_k_.erase(frame_id);
            }
            else{
                over_k_.erase(frame_id);
            }
        }
    }

}

void LRUKReplacer::Remove(frame_id_t frame_id) { 
    if(frame_id<0||frame_id>=replacer_size_){
        exit(-1);//帧的大小 小于0 或者 大于了 替换器大小，无法访问。
    }
    if(node_store_.find(frame_id)==node_store_.end()){
        // this frame_id is non-evictable;
        return;
    }
    if(!node_store_.find(frame_id)->second.is_evictable_){
        // this frame_id is non-evictable;
        exit(-1);
    }
    if(node_store_[frame_id].k_<k_)
        below_k_.erase(frame_id);
    else over_k_.erase(frame_id);
    node_store_.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t { 
    return curr_size_; 
}
bool mycmp(std::pair<frame_id_t,size_t> elem1,std::pair<frame_id_t,size_t> elem2){
    return elem1.second>elem2.second;
}
}  // namespace bustub