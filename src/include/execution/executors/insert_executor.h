//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.h
//
// Identification: src/include/execution/executors/insert_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <utility>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/insert_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * InsertExecutor executes an insert on a table.
 * Inserted values are always pulled from a child executor.
 */
class InsertExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new InsertExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The insert plan to be executed
   * @param child_executor The child executor from which inserted tuples are pulled
   */
  InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                 std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the insert */
  void Init() override;

  /**
   * Yield the number of rows inserted into the table.
   * @param[out] tuple The integer tuple indicating the number of rows inserted into the table
   * @param[out] rid The next tuple RID produced by the insert (ignore, not used)
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   *
   * NOTE: InsertExecutor::Next() does not use the `rid` out-parameter.
   * NOTE: InsertExecutor::Next() returns true with number of inserted rows produced only once.
   */
  auto Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the insert */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

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
        throw ExecutionException("InsertExecutor TryLockTable " + type + " fail");
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("InsertExecutor TryLockTable " + type + " fail");
    }
  }

  void TryUnLockTable(const table_oid_t &oid) {
    try {
      bool success = exec_ctx_->GetLockManager()->UnlockTable(exec_ctx_->GetTransaction(), oid);
      if (!success) {
        throw ExecutionException("InsertExecutor TryUnLockTable fail");
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("InsertExecutor TryUnLockTable fail");
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
        throw ExecutionException("InsertExecutor TryLockRow " + type + " fail");
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("InsertExecutor TryLockRow " + type + " fail");
    }
  }

  void TryUnLockRow(const table_oid_t &oid, const RID &rid, bool force = false) {
    try {
      bool success = exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), oid, rid, force);
      if (!success) {
        throw ExecutionException("InsertExecutor TryUnLockRow fail");
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("InsertExecutor TryUnLockRow fail");
    }
  }

 private:
  /** The insert plan node to be executed*/
  const InsertPlanNode *plan_;
  // AbstractExecutor *child_executor_;
  std::unique_ptr<AbstractExecutor> child_executor_;
  bool is_ok_{false};
};

}  // namespace bustub
