//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "catalog/catalog.h"
#include "concurrency/transaction.h"
#include "execution/executors/insert_executor.h"
#include "storage/table/table_iterator.h"
#include "storage/table/tuple.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
  // child_executor_ = child_executor.get();
}

void InsertExecutor::Init() {
  // throw NotImplementedException("InsertExecutor is not implemented");
  // 尝试加一个排他锁。
  TryLockTable(bustub::LockManager::LockMode::INTENTION_EXCLUSIVE, plan_->table_oid_);
  child_executor_->Init();
  is_ok_ = false;
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_ok_) {
    return false;
  }
  // BufferPoolManager *bpm_=exec_ctx_->GetBufferPoolManager();
  Catalog *cl = exec_ctx_->GetCatalog();
  auto &t_heap = cl->GetTable(plan_->TableOid())->table_;
  auto tableinfo = cl->GetTable(plan_->TableOid());
  // TableIterator t_iter=cl->GetTable(plan_->TableOid())->table_->MakeIterator();
  // 虽然有可以进行插入的tableheap，但是这里的 t_iter 中无法访问到  tableheap，并且远了 tableinfo->table
  // 也是一个可以拿到 heap的 东西
  auto indexsinfo = cl->GetTableIndexes(tableinfo->name_);
  int n = 0;  // 计数
  // 一个计划节点中会不会有多个值
  while (child_executor_->Next(tuple, rid)) {
    // 虽然说 t_heap->GetTupleMeta(ri) 这里可以得到 tupleMeta， 但是 ，这里得重新构造一个,
    auto ridopt = t_heap->InsertTuple({INVALID_TXN_ID, INVALID_TXN_ID, false}, *tuple, exec_ctx_->GetLockManager(),
                                      exec_ctx_->GetTransaction(), tableinfo->oid_);
    if (ridopt.has_value()) {
      // 成功插入了,将记录保存
      n++;
      // 保存表格记录
      *rid = ridopt.value();  // RID 类是有 等号赋值的
      auto twr = TableWriteRecord(tableinfo->oid_, *rid, t_heap.get());
      twr.wtype_ = WType::INSERT;
      exec_ctx_->GetTransaction()->AppendTableWriteRecord(twr);
      // 遍历这个表的索引
      for (auto &x : indexsinfo) {
        Tuple k_tuple = tuple->KeyFromTuple(tableinfo->schema_, *(x->index_->GetKeySchema()), x->index_->GetKeyAttrs());
        // insert into index. uncluster index
        x->index_->InsertEntry(k_tuple, *rid, exec_ctx_->GetTransaction());
        auto iwr =
            IndexWriteRecord{*rid, tableinfo->oid_, WType::INSERT, k_tuple, x->index_oid_, exec_ctx_->GetCatalog()};
        exec_ctx_->GetTransaction()->AppendIndexWriteRecord(iwr);
      }
    }
  }
  std::vector<Value> values{};
  values.emplace_back(Value(INTEGER, n));
  *tuple = Tuple(values, &GetOutputSchema());
  // t_heap->InsertTuple(const TupleMeta &mta, const Tuple &tule)
  is_ok_ = true;
  return true;  // child_executor_->Next(&tp, &ri) 神么情况下返回false；
}

}  // namespace bustub
