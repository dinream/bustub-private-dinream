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
  return LockTableTry(txn, lock_mode, oid, true);
}
auto LockManager::LockTableTry(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, bool ifdirector) -> bool {
  auto txn_state = txn->GetState();
  // 状态是 终止或者 已经提交，直接返回 false；
  if (txn_state == TransactionState::ABORTED || txn_state == TransactionState::COMMITTED) {
    return false;
  }
  auto iso_level = txn->GetIsolationLevel();
  // 成长阶段
  if (txn_state == TransactionState::GROWING) {
    // 对于读未提交级别的事务，只需要加 排他锁
    if (iso_level == IsolationLevel::READ_UNCOMMITTED) {
      if (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::INTENTION_EXCLUSIVE) {
        if (lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE || lock_mode == LockMode::SHARED ||
            lock_mode == LockMode::INTENTION_SHARED) {
          txn->SetState(TransactionState::ABORTED);
          throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
        }
        return false;
      }
    }
    // 对于读已提交：所有锁都可能加
    // 对于可重复读：所有锁都可能加
  }
  // 收缩阶段
  if (txn_state == TransactionState::SHRINKING) {
    // 对于可重复读，只要你释放过一个，就不能读了，更不能写了。
    if (iso_level == IsolationLevel::REPEATABLE_READ) {
      // std::cout<<""<<std::endl;
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      return false;
    }
    // 对于读已提交级别，释放过了一个写，但是还能读。但是不能写了
    if (iso_level == IsolationLevel::READ_COMMITTED) {
      if (lock_mode != LockMode::SHARED && lock_mode != LockMode::INTENTION_SHARED) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
        return false;
      }
    }
    // 对于读未提交级别，释放过一个写，能读，但是不能加读锁，所以返回 false；// 感觉还可以??? 或者是和读
    // 已提交一样，不能中间搞事情.破环原子性? 至少你写完之后,再次写.
    if (iso_level == IsolationLevel::READ_UNCOMMITTED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      return false;
    }
  }

  // 获取 当前 表  的 锁请求队列
  table_lock_map_latch_.lock();
  //   没有
  if (table_lock_map_.count(oid) == 0) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  //   有
  auto lrq = table_lock_map_[oid];
  std::unique_lock<std::mutex> lock(lrq->latch_);
  table_lock_map_latch_.unlock();

  // 判断这次的锁获取是不是一次锁升级
  bool upgrade = false;
  // for(auto iter:lrq->request_queue_){
  for (auto iter = lrq->request_queue_.begin(); iter != lrq->request_queue_.end(); iter++) {
    // auto lr = *iter;
    auto lr = *iter;
    if (lr->txn_id_ == txn->GetTransactionId()) {
      // 重复 加锁，
      if (lr->lock_mode_ == lock_mode) {
        return true;
      }
      // 检查 有正在升级的锁
      if (lrq->upgrading_ != INVALID_TXN_ID) {
        if (!ifdirector) {
          return false;
        }
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
        return false;
      }
      // 判断 不能进行锁升级
      if (!CanLockUpgrade(lr->lock_mode_, lock_mode)) {
        if (!ifdirector) {
          return false;
        }
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
  if (!upgrade) {
    lrq->request_queue_.push_back(new LockRequest(txn->GetTransactionId(), lock_mode, oid));
    // return false;
  }
  // 等待锁。
  while (!CanTxnTakeLock(txn, lock_mode, lrq)) {
    lrq->cv_.wait(lock);  // 挂起并释放 latch_
    // 等待被 notify_all 唤醒后，会重新获得 lock，然后重新判断条件
  }
  // 已经授予锁了 或者 中止了
  // 中止
  if (txn->GetState() == TransactionState::ABORTED) {
    lrq->cv_.notify_all();  // zhel   测试一下！！！！！！
    return false;
  }
  // 授予
  AddIntoTxnTableLockSet(txn, lock_mode, oid);
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  // 要解锁一个 表 首先 保证 这个表的 所有 行 都没有(已获得)加锁，正在请求的不算。
  if (!CheckAllRowsUnlock(txn, oid)) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }
  // 开始释放
  // 遍历 请求，拿到的 全 删除 ，

  // 先获取 表的 请求队列
  table_lock_map_latch_.lock();
  if (table_lock_map_.count(oid) == 0) {
    txn->SetState(TransactionState::ABORTED);
    table_lock_map_latch_.unlock();
    // table_lock_map_[oid] = std::make_shared<LockRequestQueue>();ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  auto lrq = table_lock_map_[oid];
  std::unique_lock<std::mutex> lock(lrq->latch_);
  table_lock_map_latch_.unlock();
  // 遍历请求队列
  for (auto iter = lrq->request_queue_.begin(); iter != lrq->request_queue_.end(); iter++) {
    auto lr = *iter;
    if (lr->granted_ && lr->txn_id_ == txn->GetTransactionId()) {
      auto iso_level = txn->GetIsolationLevel();
      if (iso_level == IsolationLevel::REPEATABLE_READ) {
        if (lr->lock_mode_ == LockMode::SHARED || lr->lock_mode_ == LockMode::EXCLUSIVE) {
          txn->SetState(TransactionState::SHRINKING);
        }
        // 读已提交：每次读都是一个新的开始，所以不需要变化状态，只有 写过数据，就不能写下一次数据了。没写过的才能写
      } else if (iso_level == IsolationLevel::READ_COMMITTED || iso_level == IsolationLevel::READ_UNCOMMITTED) {
        if (lr->lock_mode_ == LockMode::EXCLUSIVE) {
          txn->SetState(TransactionState::SHRINKING);
        }
      }

      RemoveFromTxnTableLockSet(txn, lr->lock_mode_, oid);

      lrq->request_queue_.erase(iter);
      delete lr;
      lrq->cv_.notify_all();

      return true;
    }
  }
  // 未定义的行为,没有加锁你来释放,故意找茬是不是
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);

  return false;
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  // 行锁 不允许 意向
  if (lock_mode != LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
  }
  auto txn_state = txn->GetState();
  auto iso_level = txn->GetIsolationLevel();
  // 事务已提交,不给锁
  if (txn_state == TransactionState::COMMITTED || txn_state == TransactionState::ABORTED) {
    return false;
  }
  // 判断这个事务的状态下,隔离级别 和 申请的 加锁类型 是否冲突
  // 加锁阶段---对于 读已提交，和，可重复读，读写锁----都可以申请的，对于读未提交，只能申请--写锁。
  // 成长阶段: 成长阶段 表锁一定是 排他的,所以都可以  读未提交,---,--后面尝试加锁的时候会排除掉这种情况
  // if(txn_state == TransactionState::GROWING){
  //   // 对于读未提交，只能 排他锁 (但是 实际上 表锁已经排它了,,这里都可以??)
  //   // 反例: 排他锁,-->发现表  有,排他,或者排他意向,, 或者 共享 排他意向才可以-->但是--无所谓,
  //    if(iso_level== IsolationLevel::READ_UNCOMMITTED){
  //     if(lock_mode != LockMode::EXCLUSIVE){
  //       txn->SetState(TransactionState::ABORTED);
  //       throw TransactionAbortException(txn->GetTransactionId(), AbortReason::);
  //     }
  //   }
  //   // 对于读已提交：共享 排他都可能加
  //   // 对于可重复读：共享 排他都可能加
  // }
  if (txn_state == TransactionState::GROWING) {
    if (iso_level == IsolationLevel::READ_UNCOMMITTED) {
      if (lock_mode == LockMode::SHARED) {
        // row: READ_UNCOMMITTED下只能获取 EXCLUSIVE 锁
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
    }
    // 如果是READ_COMMITTED或者REPEATABLE_READ隔离级别，所有的锁都能在GROWING阶段获取
  }
  // 收缩阶段
  if (txn_state == TransactionState::SHRINKING) {
    if (iso_level == IsolationLevel::REPEATABLE_READ) {  // 收缩+可重复读  ->只能 排他锁,,但是行排他锁下能在收缩用
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
    if (iso_level == IsolationLevel::READ_COMMITTED) {
      if (lock_mode != LockMode::SHARED) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
      }
    }
    if (iso_level == IsolationLevel::READ_UNCOMMITTED) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  // 判断 是否有 对应的 表锁 ，如果没有 ，请求 表锁 这里不需要等待，能就是能，能就是不能。
  if (lock_mode == LockMode::SHARED && !CheckTableOwnLock(txn, LockMode::INTENTION_SHARED, oid) &&
      !CheckTableOwnLock(txn, LockMode::SHARED, oid) &&
      !CheckTableOwnLock(txn, LockMode::SHARED_INTENTION_EXCLUSIVE, oid) &&
      !CheckTableOwnLock(txn, LockMode::INTENTION_EXCLUSIVE, oid) &&
      !CheckTableOwnLock(txn, LockMode::EXCLUSIVE, oid)) {
    // if (!LockTableTry(txn, LockMode::INTENTION_SHARED, oid, false) &&
    //     !LockTableTry(txn, LockMode::SHARED, oid,
    //                   false) &&  // 这里会排除掉，试图在 加锁阶段 的 读未提交级别，申请 共享锁
    //     !LockTableTry(txn, LockMode::SHARED_INTENTION_EXCLUSIVE, oid, false)) {
    //   txn->SetState(TransactionState::ABORTED);
    //   throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    //   return false;
    // }
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    return false;
  }
  if (lock_mode == LockMode::EXCLUSIVE && !CheckTableOwnLock(txn, LockMode::INTENTION_EXCLUSIVE, oid) &&
      !CheckTableOwnLock(txn, LockMode::EXCLUSIVE, oid) &&
      !CheckTableOwnLock(txn, LockMode::SHARED_INTENTION_EXCLUSIVE, oid)) {
    // if (!LockTableTry(txn, LockMode::INTENTION_EXCLUSIVE, oid, false) &&
    //     !LockTableTry(txn, LockMode::EXCLUSIVE, oid, false) &&
    //     !LockTableTry(txn, LockMode::SHARED_INTENTION_EXCLUSIVE, oid, false)) {
    //   // printf("err3\n");
    //   txn->SetState(TransactionState::ABORTED);
    //   throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    //   return false;
    // }
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::TABLE_LOCK_NOT_PRESENT);
    return false;
  }
  // 可以 有 对应的表锁 或者 刚刚拿到了对应的表锁。（授予了，但是没有保存）
  // 如果一个事务拿到了 表的  意向 写 锁，这里可以申请  行的 意向写锁，而且不需要进行  表锁 升级
  row_lock_map_latch_.lock();
  if (row_lock_map_.count(rid) == 0) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }

  auto lrq = row_lock_map_[rid];
  std::unique_lock<std::mutex> lock(lrq->latch_);
  row_lock_map_latch_.unlock();
  // 开始 遍历 行请求队列 判断是否 是个需要升级的请求
  bool upgrade = false;
  for (auto iter = lrq->request_queue_.begin(); iter != lrq->request_queue_.end(); iter++) {
    auto lr = *iter;
    if (lr->txn_id_ == txn->GetTransactionId()) {
      if (lr->lock_mode_ == lock_mode) {
        // 重复的锁 不需要等待授予吗？
        return true;
      }
      if (lrq->upgrading_ != INVALID_TXN_ID) {
        // 抛出 UPGRADE_CONFLICT 异常
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
      }
      if (!CanLockUpgrade(lr->lock_mode_, lock_mode)) {
        txn->SetState(TransactionState::ABORTED);
        throw TransactionAbortException(txn->GetTransactionId(), AbortReason::INCOMPATIBLE_UPGRADE);
        // 抛 INCOMPATIBLE_UPGRADE 异常
      }
      lrq->upgrading_ = txn->GetTransactionId();
      // 旧请求 删除
      lrq->request_queue_.erase(iter);
      // 旧锁 删除
      RemoveFromTxnRowLockSet(txn, lr->lock_mode_, oid, rid);
      delete lr;
      // 新 请求 进入
      lrq->request_queue_.push_back(new LockRequest{txn->GetTransactionId(), lock_mode, oid, rid});
      upgrade = true;
      break;
    }
  }
  // 新请求进入
  if (!upgrade) {
    lrq->request_queue_.push_back(new LockRequest(txn->GetTransactionId(), lock_mode, oid, rid));
  }
  // 新锁 获取
  while (!CanTxnTakeLock(txn, lock_mode, lrq)) {
    lrq->cv_.wait(lock);
  }

  if (txn->GetState() == TransactionState::ABORTED) {
    // 删除 结构？？？
    lrq->cv_.notify_all();
    return false;
  }

  AddIntoTxnRowLockSet(txn, lock_mode, oid, rid);

  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid, bool force) -> bool {
  row_lock_map_latch_.lock();
  if (row_lock_map_.count(rid) == 0) {
    row_lock_map_[rid] = std::make_shared<LockRequestQueue>();
  }

  auto lrq = row_lock_map_[rid];
  std::unique_lock<std::mutex> lock(lrq->latch_);
  row_lock_map_latch_.unlock();

  for (auto iter = lrq->request_queue_.begin(); iter != lrq->request_queue_.end(); iter++) {
    auto lr = *iter;
    if (lr->granted_ && lr->txn_id_ == txn->GetTransactionId()) {
      if (!force) {
        auto iso_level = txn->GetIsolationLevel();
        if (iso_level == IsolationLevel::REPEATABLE_READ) {
          if (lr->lock_mode_ == LockMode::SHARED || lr->lock_mode_ == LockMode::EXCLUSIVE) {
            txn->SetState(TransactionState::SHRINKING);
          }
        } else if (iso_level == IsolationLevel::READ_COMMITTED || iso_level == IsolationLevel::READ_UNCOMMITTED) {
          if (lr->lock_mode_ == LockMode::EXCLUSIVE) {
            txn->SetState(TransactionState::SHRINKING);
          }
        }
      }
      RemoveFromTxnRowLockSet(txn, lr->lock_mode_, oid, rid);
      lrq->request_queue_.erase(iter);
      delete lr;
      lrq->cv_.notify_all();
      return true;
    }
  }

  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);

  return false;
}

void LockManager::UnlockAll() {
  // You probably want to unlock all table and txn locks here.
  // You probably want to unlock all table and txn locks here.
  for (auto &[k, v] : row_lock_map_) {
    for (auto lr : v->request_queue_) {
      /*if (lr->granted_) {
       ReomoveTxnRowLockSet(txn_manager_->GetTransaction(lr->txn_id_), lr->lock_mode_, lr->oid_, lr->rid_);
     }*/
      delete lr;
    }
  }

  for (auto &[k, v] : table_lock_map_) {
    for (auto lr : v->request_queue_) {
      /*if (lr->granted_) {
       RemoveFromTxnTableLockSet(txn_manager_->GetTransaction(lr->txn_id_), lr->lock_mode_, lr->oid_);
     }*/
      delete lr;
    }
  }
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  // std::unique_lock<std::mutex> lock(waits_for_latch_);
  waits_for_[t1].emplace_back(t2);
  // if (std::find(all_node_.begin(), all_node_.end(), t1) == all_node_.end()) {
  //   all_node_.insert(t1);
  // }
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  // std::unique_lock<std::mutex> lock(waits_for_latch_);
  // std::unordered_map<txn_id_t, std::vector<txn_id_t>> waits_for_;
  waits_for_[t1].erase(std::find(waits_for_[t1].begin(), waits_for_[t1].end(), t2));
  // if (waits_for_[t1].empty()) {
  //   all_node_.erase(std::find(all_node_.begin(), all_node_.end(), t1));
  // }
}

// 使用深度优先搜索（DFS）寻找循环。
// 如果找到一个循环， HasCycle 应该将循环中最新交易的交易ID存储在 txn_id 中并返回true。
// 您的函数应该返回它找到的第一个循环。如果您的图表没有循环， HasCycle 应返回 false。
auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  // 初始化访问列表

  for (auto &[kkk, v] : waits_for_) {
    auto k = kkk;
    for (auto &[kk, vv] : waits_for_) {
      is_visited_[kk] = 0;
    }
    std::cout << k << std::endl;
    if (is_visited_[k] == 0 && HasCycleByDfs(k)) {
      *txn_id = k;
      return true;
    }
  }
  printf("there is no cycle\n");

  return false;
}
auto LockManager::HasCycleByDfs(txn_id_t &txn_id) -> bool {
  is_visited_[txn_id] = 1;  // GREY
  for (auto x : waits_for_[txn_id]) {
    if (is_visited_[x] == 1) {
      is_visited_[x] = 3;
      if (x > txn_id) {
        txn_id = x;
      }
      return true;
    }
    if (is_visited_[x] == 0) {
      auto y = x;
      if (HasCycleByDfs(y)) {
        if (is_visited_[x] == 3 || y > txn_id) {
          txn_id = y;
        }
        return true;
      }
    }
  }

  is_visited_[txn_id] = 2;  // 2 表示 从这个地方出去没有找到环
  return false;
}

// 返回表示图中边的元组列表
auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  for (auto &[k, v] : waits_for_) {
    for (auto x : v) {
      edges.emplace_back(k, x);
    }
  }
  return edges;
}
void LockManager::BuildGraph() {
  table_lock_map_latch_.lock();
  for (auto &[k, v] : table_lock_map_) {
    v->latch_.lock();
    for (auto iter1 = v->request_queue_.begin(); iter1 != v->request_queue_.end(); iter1++) {
      for (auto lr2 : v->request_queue_) {
        auto lr1 = *iter1;
        if (txn_manager_->GetTransaction(lr1->txn_id_)->GetState() != TransactionState::ABORTED &&
            txn_manager_->GetTransaction(lr2->txn_id_)->GetState() != TransactionState::ABORTED && !lr1->granted_ &&
            lr2->granted_ && !AreLocksCompatible(lr1->lock_mode_, lr2->lock_mode_)) {
          AddEdge(lr1->txn_id_, lr2->txn_id_);
        }
      }
    }
    v->latch_.unlock();
  }
  table_lock_map_latch_.unlock();

  row_lock_map_latch_.lock();
  for (auto &[k, v] : row_lock_map_) {
    v->latch_.lock();
    for (auto iter1 = v->request_queue_.begin(); iter1 != v->request_queue_.end(); iter1++) {
      for (auto lr2 : v->request_queue_) {
        auto lr1 = *iter1;
        if (!lr1->granted_ && lr2->granted_ && !AreLocksCompatible(lr1->lock_mode_, lr2->lock_mode_)) {
          AddEdge(lr1->txn_id_, lr2->txn_id_);
        }
      }
    }
    v->latch_.unlock();
  }
  row_lock_map_latch_.unlock();
}

// 包含后台运行周期检测的骨架代码。您应该在这里实现周期检测算法
void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {  // TODO(students): detect deadlock
      BuildGraph();
      while (true) {
        is_visited_.clear();

        txn_id_t abort_tid;
        if (HasCycle(&abort_tid)) {
          txn_manager_->GetTransaction(abort_tid)->SetState(TransactionState::ABORTED);
          RemoveAllAboutAbortTxn(abort_tid);
        } else {
          break;
        }
      }
    }
    waits_for_.clear();
  }
}

void LockManager::RemoveAllAboutAbortTxn(txn_id_t abort_id) {
  table_lock_map_latch_.lock();
  for (auto &[k, lrq] : table_lock_map_) {
    lrq->latch_.lock();

    for (auto iter = lrq->request_queue_.begin(); iter != lrq->request_queue_.end();) {
      auto lr = *iter;
      if (lr->txn_id_ == abort_id) {
        lrq->request_queue_.erase(iter++);
        if (lr->granted_) {
          RemoveFromTxnTableLockSet(txn_manager_->GetTransaction(abort_id), lr->lock_mode_, lr->oid_);
          lrq->cv_.notify_all();
        }
        delete lr;
      } else {
        iter++;
      }
    }

    lrq->latch_.unlock();
  }
  table_lock_map_latch_.unlock();

  row_lock_map_latch_.lock();
  for (auto &[k, lrq] : row_lock_map_) {
    lrq->latch_.lock();
    for (auto iter = lrq->request_queue_.begin(); iter != lrq->request_queue_.end();) {
      auto lr = *iter;
      if (lr->txn_id_ == abort_id) {
        lrq->request_queue_.erase(iter++);
        if (lr->granted_) {
          RemoveFromTxnRowLockSet(txn_manager_->GetTransaction(abort_id), lr->lock_mode_, lr->oid_, lr->rid_);
          lrq->cv_.notify_all();
        }
        delete lr;
      } else {
        iter++;
      }
    }
    lrq->latch_.unlock();
  }
  row_lock_map_latch_.unlock();

  waits_for_.erase(abort_id);

  for (auto iter = waits_for_.begin(); iter != waits_for_.end();) {
    if (std::find((*iter).second.begin(), (*iter).second.end(), abort_id) != (*iter).second.end()) {
      RemoveEdge((*iter).first, abort_id);
    }
    if ((*iter).second.empty()) {
      waits_for_.erase(iter++);
    } else {
      iter++;
    }
  }
}

auto LockManager::UpgradeLockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  return false;
}

auto LockManager::UpgradeLockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  return false;
}
// 判断 1 锁 是否 可以  兼容  2 锁 ，2 是新的
auto LockManager::AreLocksCompatible(LockMode l2, LockMode l1) -> bool {
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

// 从 事务 的 已锁 表 队列中 删除
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
// 从 事务 的 已锁 行 队列中 删除
void LockManager::RemoveFromTxnRowLockSet(Transaction *txn, LockMode lock_mode, const table_oid_t &oid,
                                          const RID &rid) {
  // std::shared_ptr<std::unordered_map<table_oid_t, std::unordered_set<RID>>> s_row_lock_set_;
  if (lock_mode == LockMode::SHARED) {
    (*(txn->GetSharedRowLockSet()))[oid].erase(rid);
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    (*(txn->GetExclusiveRowLockSet()))[oid].erase(rid);
  }
}
// 添加到 事务的 已锁 表 队列
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
// 添加到 事务的 已锁 行 队列
void LockManager::AddIntoTxnRowLockSet(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) {
  if (lock_mode == LockMode::SHARED) {
    (*(txn->GetSharedRowLockSet()))[oid].insert(rid);
  } else if (lock_mode == LockMode::EXCLUSIVE) {
    // txn->GetExclusiveTableLockSet()->insert(oid);
    (*(txn->GetExclusiveRowLockSet()))[oid].insert(rid);
  }
}

// 等待条件：判断是否可以赋予锁
auto LockManager::CanTxnTakeLock(Transaction *txn, LockMode lock_mode,
                                 std::shared_ptr<LockRequestQueue> &lock_request_queue) -> bool {
  // 被其他线程 终止了,从请求队列中清除；
  if (txn->GetState() == TransactionState::ABORTED) {
    if (lock_request_queue->upgrading_ == txn->GetTransactionId()) {
      lock_request_queue->upgrading_ = INVALID_TXN_ID;
    }
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
    if (lr->granted_ && lr->txn_id_ != txn->GetTransactionId() && !AreLocksCompatible(lock_mode, lr->lock_mode_)) {
      return false;
    }
  }
  // 说明 现在 事务 就可以加锁了
  // 找到这个事务，并且考虑 赋予锁。
  for (auto &lr : lock_request_queue->request_queue_) {
    // 当前事务没有赋予锁
    if (!lr->granted_ && lr->txn_id_ == txn->GetTransactionId()) {
      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        if (lock_request_queue->upgrading_ != txn->GetTransactionId()) {
          // 正在升级 别的 锁，等待
          return false;
        }
        lock_request_queue->upgrading_ = INVALID_TXN_ID;
        lr->granted_ = true;
        return true;
      }
    }
  }
  for (auto &lr : lock_request_queue->request_queue_) {
    // 其他事务（在当前事务前面）没有赋予锁，但是和当前锁不兼容， 不给。

    if (!lr->granted_ && lr->txn_id_ != txn->GetTransactionId()) {
      if (!AreLocksCompatible(lock_mode, lr->lock_mode_)) {
        return false;
      }
    }
    if (!lr->granted_ && lr->txn_id_ == txn->GetTransactionId()) {
      lr->granted_ = true;
      return true;
    }
  }
  return false;
  // if (lock_mode == LockMode::SHARED) {
  //   std::cout << "a" << std::endl;
  // }
  // bool all_locks_compatible =
  //     std::all_of(lock_request_queue->request_queue_.begin(), lock_request_queue->request_queue_.end(), [&](auto &lr)
  //     {
  //       if (!lr->granted_ && lr->txn_id_ != txn->GetTransactionId()) {
  //         return AreLocksCompatible(lock_mode, lr->lock_mode_);
  //       }
  //       return true;
  //     });

  // return all_locks_compatible;
}

// 判断是否 事务 拥有 表锁
auto LockManager::CheckTableOwnLock(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  table_lock_map_latch_.lock();
  auto lrq = table_lock_map_[oid];
  table_lock_map_latch_.unlock();

  lrq->latch_.lock();
  for (auto iter = lrq->request_queue_.begin(); iter != lrq->request_queue_.end(); iter++) {
    auto lr = *iter;
    if (lr->granted_ && lr->txn_id_ == txn->GetTransactionId() && lr->lock_mode_ == lock_mode) {
      lrq->latch_.unlock();
      return true;
    }
  }
  lrq->latch_.unlock();
  return false;
}
void LockManager::GrantNewLocksIfPossible(LockRequestQueue *lock_request_queue) {}

// 是否可以进行锁升级
auto LockManager::CanLockUpgrade(LockMode curr_lock_mode, LockMode requested_lock_mode) -> bool {
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
// 判断 当前事务  是否对所有行 都没加锁
auto LockManager::CheckAllRowsUnlock(Transaction *txn, const table_oid_t &oid) -> bool {
  row_lock_map_latch_.lock();
  for (auto [k, v] : row_lock_map_) {
    row_lock_map_latch_.unlock();
    v->latch_.lock();
    for (auto iter = v->request_queue_.begin(); iter != v->request_queue_.end(); iter++) {
      if ((*iter)->txn_id_ == txn->GetTransactionId() && (*iter)->oid_ == oid && (*iter)->granted_) {
        v->latch_.unlock();
        // row_lock_map_latch_.unlock();  // 感觉有问题
        return false;
      }
    }
    v->latch_.unlock();
    row_lock_map_latch_.lock();
  }

  row_lock_map_latch_.unlock();
  return true;
}

auto LockManager::CheckAppropriateLockOnTable(Transaction *txn, const table_oid_t &oid, LockMode row_lock_mode)
    -> bool {
  return false;
}
auto LockManager::FindCycle(txn_id_t source_txn, std::vector<txn_id_t> &path, std::unordered_set<txn_id_t> &on_path,
                            std::unordered_set<txn_id_t> &visited, txn_id_t *abort_txn_id) -> bool {
  return false;
}

}  // namespace bustub
