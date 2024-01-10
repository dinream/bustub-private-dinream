//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include <memory>
#include "buffer/buffer_pool_manager.h"
#include "catalog/catalog.h"
#include "common/macros.h"
#include "common/rid.h"
#include "concurrency/transaction_manager.h"
#include "storage/index/b_plus_tree.h"
#include "storage/table/table_heap.h"
#include "storage/table/table_iterator.h"
#include "type/value_factory.h"

namespace bustub {
/**
* ecec_ctx 执行器上下文，由外部构造，此处保存指针
* catalog_{catalog},
  bpm_{bpm}, 比较重要的
* plan 序列扫描，由外部构造，此处保存指针

*/
SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  // t_iter_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->MakeIterator();
  // std::cout<<"haha"<<std::endl;
}

void SeqScanExecutor::Init() {
  // throw NotImplementedException("SeqScanExecutor is not implemented");
  // Catalog *cl = exec_ctx_->GetCatalog();
  t_id_ = plan_->GetTableOid();
  // 如果 exec_ctx_ 中 需要对 元组进行删除，这里 就需要 获取  exclusiveTable 锁。
  if (exec_ctx_->IsDelete()) {
    TryLockTable(bustub::LockManager::LockMode::INTENTION_EXCLUSIVE, t_id_);
  } else {
    if (exec_ctx_->GetTransaction()->GetIntentionExclusiveTableLockSet()->count(t_id_) == 0 &&
        exec_ctx_->GetTransaction()->GetExclusiveTableLockSet()->count(t_id_) == 0) {
      auto iso_level = exec_ctx_->GetTransaction()->GetIsolationLevel();
      // 对于读如果不是READ_UNCOMMITTED就需要获取 INTENTION_SHARED 锁 READ_UNCOMMITED 不需要加 SHARED 锁
      if (iso_level == IsolationLevel::READ_COMMITTED || iso_level == IsolationLevel::REPEATABLE_READ) {
        TryLockTable(bustub::LockManager::LockMode::INTENTION_SHARED, t_id_);
      }
    }  /// GetExclusiveTableLockSet()->count(t_id_) == 0
  }
  TableInfo *table = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());

  // BUSTUB_ASSERT(table == nullptr, "table is not exist ");
  auto tb = table->table_.get();
  t_iter_ = std::make_unique<TableIterator>(tb->MakeEagerIterator());
  // *t_iter_ = (table->table_->MakeIterator());
  // *t_iter_ = (table->table_->MakeIterator());
  // t_iter_ = new TableIterator(table->table_->MakeIterator());
  // t_iter_ = table->table_->MakeIterator();
  // t_iter_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_->MakeEagerIterator();
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {  // 主要是为了防止 被删除的元组连续出现
    if (t_iter_->IsEnd()) {
      // if (t_iter_ != nullptr) {  // 防止多次 Next 导致多次释放  ,
      //   delete t_iter_;  // 以前的实现方式，用  new  构造迭代器，比较 容易内存泄漏
      //   t_iter_ = nullptr;
      // }
      auto iso_level = exec_ctx_->GetTransaction()->GetIsolationLevel();
      // 在 READ_COMMITTED 下，在 Next() 函数中，若表中已经没有数据，则提前释放之前持有的锁, w
      // 为什么 READ_UNCOMMITED 级别下，不需要去释放 写锁 。如果你不释放写锁，其他 怎么读你未提交的数据
      // ，，，不需要！！！ 因为为什么呢，其他人根本就不会加读锁，所以压根不冲突，谁便读。 但是 为什么要
      // 加这个写锁呢。，因为 is Delete 下 上层算子需要删除，扫描到最后一个，了，涉及到删除的最后一个算子
      // 这里正确的理解应该是：1. 不涉及写，所有人都没有写锁。 2. 可重复读不能释放读锁。 3. 读未提交没有加读锁。
      if (iso_level == IsolationLevel::READ_COMMITTED && !exec_ctx_->IsDelete()) {
        TryUnLockTable(t_id_);
      }
      // 如果是 IsDelete true 三种级别都是加的 写锁，为什么不释放。
      // 可重复读：不能释放读写锁，只有提交的时候才能释放
      // 读已提交：不能释放写锁，防止 其他人读你还没有提交的表，
      // 读未提交：不能释放写锁，防止 其他人修改，你读到的数据 和 你 自己修改的结果不一致。
      return false;
    }
    // 对于每一个行，尝试加锁
    *tuple = t_iter_->GetTuple().second;
    if (exec_ctx_->IsDelete()) {
      TryLockRow(bustub::LockManager::LockMode::EXCLUSIVE, t_id_, tuple->GetRid());
    } else {
      // 如果 IsDelete() 为 false 并且 当前行未被 X lock 锁住 并且隔离级别不为 READ_UNCOMMITTED，则对行上 S lock
      if ((*exec_ctx_->GetTransaction()->GetExclusiveRowLockSet())[t_id_].count(tuple->GetRid()) == 0) {
        auto iso_level = exec_ctx_->GetTransaction()->GetIsolationLevel();
        // 对于读如果不是READ_UNCOMMITTED就需要获取 SHARED锁
        if (iso_level == IsolationLevel::READ_COMMITTED || iso_level == IsolationLevel::REPEATABLE_READ) {
          TryLockRow(bustub::LockManager::LockMode::SHARED, t_id_, tuple->GetRid());
        }
      }
    }
    // 加锁之后只干一件事情。扫描一下这个 元组的有效性 ++++ 算子决定了， 加了行锁 也不会干什么特别的事情  +++ 走走形式？
    // std::cout << "hah" << std::endl;
    // ||
    //     (plan_->filter_predicate_ != nullptr &&
    //      plan_->filter_predicate_->Evaluate(tuple, exec_ctx_->GetCatalog()->GetTable(t_id_)->schema_)
    //              .CompareEquals(ValueFactory::GetBooleanValue(false)) ==
    //          CmpBool::CmpTrue)
    if (t_iter_->GetTuple().first.is_deleted_) {  // 出现 where 还需要比较一下
      auto iso_level = exec_ctx_->GetTransaction()->GetIsolationLevel();
      if (iso_level != IsolationLevel::READ_UNCOMMITTED) {
        TryUnLockRow(t_id_, tuple->GetRid(), true);
      }
      ++(*t_iter_);  // 下移
      continue;      // 当前元组无效
    }
    // 然后就尝试解锁没有用的锁了。（其实可以考虑直接就只加锁，一会就不用解锁了）
    auto iso_level = exec_ctx_->GetTransaction()->GetIsolationLevel();
    // 如果 IsDelete() 为 false 并且 隔离级别为 READ_COMMITTED ，还可以释放所有的 S 锁
    //
    if (iso_level == IsolationLevel::READ_COMMITTED && !exec_ctx_->IsDelete()) {
      TryUnLockRow(t_id_, tuple->GetRid(), false);
    }

    *tuple = t_iter_->GetTuple().second;
    *rid = tuple->GetRid();
    break;
  }
  ++(*t_iter_);
  // return !((++t_iter_)->IsEnd()); 只是要返回了 数据 ，说明 就是 false
  return true;
}

}  // namespace bustub
