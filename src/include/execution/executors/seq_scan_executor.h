//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.h
//
// Identification: src/include/execution/executors/seq_scan_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/rid.h"
#include "concurrency/transaction.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "storage/table/table_iterator.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The SeqScanExecutor executor executes a sequential table scan.
 */
class SeqScanExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SeqScanExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sequential scan plan to be executed
   */
  SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan);

  /** Initialize the sequential scan */
  void Init() override;

  /**
   * Yield the next tuple from the sequential scan.
   * @param[out] tuple The next tuple produced by the scan
   * @param[out] rid The next tuple RID produced by the scan
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the sequential scan */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  void TryLockTable(const bustub::LockManager::LockMode &lock_mode, const table_oid_t &oid) {
    std::string type;
    if (lock_mode == bustub::LockManager::LockMode::EXCLUSIVE) {
      type = "X";
    } else if (lock_mode == bustub::LockManager::LockMode::INTENTION_EXCLUSIVE) {
      type = "IX";
    } else if (lock_mode == bustub::LockManager::LockMode::INTENTION_SHARED) {
      type = "IS";
    } else if (lock_mode == bustub::LockManager::LockMode::SHARED) {
      type = "S";
    }

    try {
      bool success = exec_ctx_->GetLockManager()->LockTable(exec_ctx_->GetTransaction(), lock_mode, oid);
      if (!success) {
        throw ExecutionException("SeqScanExecutor TryLockTable " + type + " fail");
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("SeqScanExecutor TryLockTable " + type + " fail");
    }
  }

  void TryUnLockTable(const table_oid_t &oid) {
    try {
      bool success = exec_ctx_->GetLockManager()->UnlockTable(exec_ctx_->GetTransaction(), oid);
      if (!success) {
        throw ExecutionException("SeqScanExecutor TryUnLockTable fail");
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("SeqScanExecutor TryUnLockTable fail");
    }
  }

  void TryLockRow(const bustub::LockManager::LockMode &lock_mode, const table_oid_t &oid, const RID &rid) {
    std::string type;
    if (lock_mode == bustub::LockManager::LockMode::EXCLUSIVE) {
      type = "X";
    } else if (lock_mode == bustub::LockManager::LockMode::SHARED) {
      type = "S";
    }

    try {
      bool success = exec_ctx_->GetLockManager()->LockRow(exec_ctx_->GetTransaction(), lock_mode, oid, rid);
      if (!success) {
        throw ExecutionException("SeqScanExecutor TryLockRow " + type + " fail");
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("SeqScanExecutor TryLockRow " + type + " fail");
    }
  }

  void TryUnLockRow(const table_oid_t &oid, const RID &rid, bool force = false) {
    try {
      bool success = exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), oid, rid, force);
      if (!success) {
        throw ExecutionException("SeqScanExecutor TryUnLockRow fail");
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("SeqScanExecutor TryUnLockRow fail");
    }
  }

 private:
  /** The sequential scan plan node to be executed */
  const SeqScanPlanNode *plan_;
  table_oid_t t_id_{0};
  std::unique_ptr<TableIterator> t_iter_;
};
}  // namespace bustub
