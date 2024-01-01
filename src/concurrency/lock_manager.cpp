//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool { 
  auto txn_state = txn->GetState();
  // 状态是 终止或者 已经提交，直接返回 false；
  if(txn_state == TransactionState::ABORTED || txn_state==TransactionState::COMMITTED){
    return false;
  }
  auto iso_level= txn->GetIsolationLevel();
  // 成长阶段
  if(txn_state == TransactionState::GROWING){
    // 对于读未提交级别的事务，只需要加 排他锁
    if(iso_level== IsolationLevel::READ_UNCOMMITTED){
      if(lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::INTENTION_EXCLUSIVE){
        return false;
      }
    }
    // 对于读已提交：所有锁都可能加
    // 对于可重复读：所有锁都可能加
  }
  // 收缩阶段
  else if(txn_state == TransactionState::SHRINKING){
    // 对于可重复读，不管你什么锁，都不许收缩
    if(iso_level == IsolationLevel::REPEATABLE_READ){
      std::cout<<""<<std::endl;
      return false;
    }
    // 对于读已提交级别，只能 收缩 共享锁
    if(iso_level == IsolationLevel::READ_COMMITTED){
      if(lock_mode != LockMode::SHARED && lock_mode != LockMode::INTENTION_SHARED){
        return false;
      }
    }
    // 对于读未提交级别，本来就只有一个，排他，还不能收缩
    if(iso_level == IsolationLevel::READ_UNCOMMITTED){
      return false;
    }
  }

  // 获取 当前 表  的 锁请求队列
  table_lock_map_latch_.lock();
  //   没有
  if(table_lock_map_.count(oid) == 0){
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  //   有
  auto lrq = table_lock_map_[oid];
  std::unique_lock<std::mutex> lock(lrq->latch_);
  table_lock_map_latch_.unlock();

  // 判断这次的锁获取是不是一次锁升级
  bool upgrade = false;
  //for(auto iter:lrq->request_queue_){
  for (auto iter = lrq->request_queue_.begin(); iter != lrq->request_queue_.end();
         iter++) {
      // auto lr = *iter;
    auto lr = *iter;
    if(lr->txn_id_==txn->GetTransactionId()){
      // 重复 加锁，
      if(lr->lock_mode_==lock_mode){
        return true; 
      }
      // 检查 有正在升级的锁
      if(lrq->upgrading_!=INVALID_TXN_ID){
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
        return false;
      }
      // 判断 不能进行锁升级
      if(!CanLockUpgrade(lr->lock_mode_,lock_mode)){
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
        return false;
      }
      // 开始升级
      upgrade = true;
      lrq->upgrading_ = txn->GetTransactionId();
      // 删除旧请求
      lrq->request_queue_.erase(iter);
      // 从表锁集合删除 旧 锁
      RemoveFromTxnTableLockSet(txn, lr->lock_mode_, oid);
      // 释放旧的请求
      delete lr;
      // 保存新的请求
      lrq->request_queue_.push_back(new LockRequest{txn->GetTransactionId(), lock_mode, oid});
      break;
    }
  }
  // 不是 锁升级，作为普通请求也要保存
  if(!upgrade){
    lrq->request_queue_.push_back(new LockRequest(txn->GetTransactionId(), lock_mode, oid));
    // return false;
  }
  // 等待锁。
  while(!CanTxnTakeLock(txn, lock_mode,lrq)){
    lrq->cv_.wait(lock);
  }
  // 已经授予锁了 或者 中止了
  // 中止
  if (txn->GetState() == TransactionState::ABORTED) {
    lrq->cv_.notify_all();
    return false;
  }
  // 授予
  AddIntoTxnTableLockSet(txn, lock_mode, oid);
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool { 

  return true; 
  
  }

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  return true;
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {

}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {

}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool { 
  return false; 
  }

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
    }
  }
}

auto LockManager::UpgradeLockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool{
  return false;
}

auto LockManager::UpgradeLockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool{
  return false;
}
// 判断 1 锁 是否 可以  兼容  2 锁 ，2 是新的
auto LockManager::AreLocksCompatible(LockMode l1, LockMode l2) -> bool{
  if (l1 == LockMode::INTENTION_SHARED) {
    return l2 == LockMode::INTENTION_SHARED || l2 == LockMode::INTENTION_EXCLUSIVE || l2 == LockMode::SHARED ||
           l2 == LockMode::SHARED_INTENTION_EXCLUSIVE;
  } 
  if (l1 == LockMode::INTENTION_EXCLUSIVE) {
    return l2 == LockMode::INTENTION_SHARED || l2 == LockMode::INTENTION_EXCLUSIVE;
  } 
  if (l1 == LockMode::SHARED) {
    return l2 == LockMode::INTENTION_SHARED || l2 == LockMode::SHARED;
  } 
  if (l1 == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    return l2 == LockMode::INTENTION_SHARED;
  }
  return false;
}

// 从 事务 的 已锁 队列中 删除
void LockManager::RemoveFromTxnTableLockSet(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) {
  if (lock_mode == LockMode::SHARED) {
    txn->GetSharedTableLockSet()->erase(oid);
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    txn->GetExclusiveTableLockSet()->erase(oid);
  } else if (lock_mode == LockMode::INTENTION_SHARED) {
    txn->GetIntentionSharedTableLockSet()->erase(oid);
  } else if (lock_mode == LockMode::INTENTION_EXCLUSIVE) {
    txn->GetIntentionExclusiveTableLockSet()->erase(oid);
  } else if (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->GetSharedIntentionExclusiveTableLockSet()->erase(oid);
  }
}
// 添加到 事务的 已锁队列
void LockManager::AddIntoTxnTableLockSet(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) {
  if (lock_mode == LockMode::SHARED) {
    txn->GetSharedTableLockSet()->insert(oid);
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    txn->GetExclusiveTableLockSet()->insert(oid);
  } else if (lock_mode == LockMode::INTENTION_SHARED) {
    // printf("txn %d is\n", txn->GetTransactionId());
    txn->GetIntentionSharedTableLockSet()->insert(oid);
  } else if (lock_mode == LockMode::INTENTION_EXCLUSIVE) {
    txn->GetIntentionExclusiveTableLockSet()->insert(oid);
  } else if (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    txn->GetSharedIntentionExclusiveTableLockSet()->insert(oid);
  }
}

// 等待条件：判断是否可以赋予锁
auto LockManager::CanTxnTakeLock(Transaction *txn, LockMode lock_mode, std::shared_ptr<LockRequestQueue> &lock_request_queue) -> bool{
  // 被其他线程 终止了
  if(txn->GetState()==TransactionState::ABORTED){
    for (auto iter = lock_request_queue->request_queue_.begin(); iter != lock_request_queue->request_queue_.end();
         iter++) {
      auto lr = *iter;
      if (lr->txn_id_ == txn->GetTransactionId()) {
        lock_request_queue->request_queue_.erase(iter);
        delete lr;
        break;
      }
    }
    return true;
  }
  // 判断当前拿到锁的事务，是否 兼容 这个 事务 锁
  for (auto lr : lock_request_queue->request_queue_) {
    if (lr->granted_ && !AreLocksCompatible(lock_mode, lr->lock_mode_)) {
       return false;
    }
  }
  // 说明 现在 事务 就可以加锁了

    // 找到这个事务，并且考虑 赋予锁。
      
  for (auto & lr : lock_request_queue->request_queue_) {
    // 当前事务没有赋予锁
    if (!lr->granted_ && lr->txn_id_ == txn->GetTransactionId()) {
      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        if(lock_request_queue->upgrading_ != txn->GetTransactionId()){
          // 正在升级 别的 锁，等待
          return false;
        }
        lock_request_queue->upgrading_ = INVALID_TXN_ID;
      }
      lr->granted_ = true;
      return true;
    }
    // 其他事务（在当前事务前面）没有赋予锁，但是和当前锁不兼容， 不给。
    if (!lr->granted_ && lr->txn_id_ != txn->GetTransactionId()) {
      if (!AreLocksCompatible(lock_mode, lr->lock_mode_)) {
        return false;
      }
    }
  }
  return true;
}

void LockManager::GrantNewLocksIfPossible(LockRequestQueue *lock_request_queue){
  
}

auto LockManager::CanLockUpgrade(LockMode curr_lock_mode, LockMode requested_lock_mode) -> bool{
  if (curr_lock_mode == LockMode::INTENTION_SHARED) {
    return requested_lock_mode == LockMode::EXCLUSIVE || requested_lock_mode == LockMode::SHARED ||
           requested_lock_mode == LockMode::INTENTION_EXCLUSIVE ||
           requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE;
  } 
  if (curr_lock_mode == LockMode::SHARED) {
    return requested_lock_mode == LockMode::EXCLUSIVE || requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE;
  } 
  if (curr_lock_mode == LockMode::INTENTION_EXCLUSIVE) {
    return requested_lock_mode == LockMode::EXCLUSIVE || requested_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE;
  } 
  if (curr_lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
    return requested_lock_mode == LockMode::EXCLUSIVE;
  }
  return false;
}

auto LockManager::CheckAppropriateLockOnTable(Transaction *txn, const table_oid_t &oid, LockMode row_lock_mode) -> bool{
  return false;
}
auto LockManager::FindCycle(txn_id_t source_txn, std::vector<txn_id_t> &path, std::unordered_set<txn_id_t> &on_path, std::unordered_set<txn_id_t> &visited, txn_id_t *abort_txn_id) -> bool{
  return false;
}

}  // namespace bustub
