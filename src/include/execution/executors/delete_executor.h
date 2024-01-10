//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.h
//
// Identification: src/include/execution/executors/delete_executor.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/delete_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * DeletedExecutor executes a delete on a table.
 * Deleted values are always pulled from a child.
 */
class DeleteExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new DeleteExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The delete plan to be executed
   * @param child_executor The child executor that feeds the delete
   */
  DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                 std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the delete */
  void Init() override;

  /**
   * Yield the number of rows deleted from the table.
   * @param[out] tuple The integer tuple indicating the number of rows deleted from the table
   * @param[out] rid The next tuple RID produced by the delete (ignore, not used)
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   *
   * NOTE: DeleteExecutor::Next() does not use the `rid` out-parameter.
   * NOTE: DeleteExecutor::Next() returns true with the number of deleted rows produced only once.
   */
  auto Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the delete */
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
        throw ExecutionException("DeleteExecutor TryLockTable " + type + " fail");
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("DeleteExecutor TryLockTable " + type + " fail");
    }
  }

  void TryUnLockTable(const table_oid_t &oid) {
    try {
      bool success = exec_ctx_->GetLockManager()->UnlockTable(exec_ctx_->GetTransaction(), oid);
      if (!success) {
        throw ExecutionException("DeleteExecutor TryUnLockTable fail");
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("DeleteExecutor TryUnLockTable fail");
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
        throw ExecutionException("DeleteExecutor TryLockRow " + type + " fail");
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("DeleteExecutor TryLockRow " + type + " fail");
    }
  }

  void TryUnLockRow(const table_oid_t &oid, const RID &rid, bool force = false) {
    try {
      bool success = exec_ctx_->GetLockManager()->UnlockRow(exec_ctx_->GetTransaction(), oid, rid, force);
      if (!success) {
        throw ExecutionException("DeleteExecutor TryUnLockRow fail");
      }
    } catch (TransactionAbortException &e) {
      throw ExecutionException("DeleteExecutor TryUnLockRow fail");
    }
  }

 private:
  /** The delete plan node to be executed */
  const DeletePlanNode *plan_;
  /** The child executor from which RIDs for deleted tuples are pulled */
  std::unique_ptr<AbstractExecutor> child_executor_;
  bool is_ok_{false};
};
}  // namespace bustub
