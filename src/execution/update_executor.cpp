//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
  // child_executor_=child_executor.get();
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  child_executor_->Init();
  is_ok_ = false;
  // throw NotImplementedException("UpdateExecutor is not implemented");
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_ok_) {
    return false;
  }
  // BufferPoolManager *bpm_=exec_ctx_->GetBufferPoolManager();
  Catalog *cl = exec_ctx_->GetCatalog();
  auto &t_heap = cl->GetTable(plan_->TableOid())->table_;
  table_info_ = cl->GetTable(plan_->TableOid());
  auto indexsinfo = cl->GetTableIndexes(table_info_->name_);
  int n = 0;                                   // 计数
  while (child_executor_->Next(tuple, rid)) {  // 不同列？
    // 确定要更新的目标元组，可能有多个
    std::vector<Value> values{};
    for (auto &exp : plan_->target_expressions_) {
      values.push_back(exp->Evaluate(tuple, table_info_->schema_));
    }
    Tuple new_tuple = Tuple(values, &table_info_->schema_);
    t_heap->UpdateTupleInPlaceUnsafe(TupleMeta{INVALID_TXN_ID, INVALID_TXN_ID, false}, new_tuple, *rid);
    n++;

    auto twr = TableWriteRecord{table_info_->oid_, *rid, table_info_->table_.get()};
    twr.wtype_ = WType::UPDATE;

    exec_ctx_->GetTransaction()->GetWriteSet()->push_back(twr);
    for (auto &x : indexsinfo) {
      Tuple partial_tuple =
          tuple->KeyFromTuple(table_info_->schema_, *(x->index_->GetKeySchema()), x->index_->GetKeyAttrs());
      x->index_->DeleteEntry(partial_tuple, *rid, exec_ctx_->GetTransaction());
      auto iwr1 = IndexWriteRecord{*rid,          table_info_->oid_, WType::DELETE,
                                   partial_tuple, x->index_oid_,     exec_ctx_->GetCatalog()};
      exec_ctx_->GetTransaction()->GetIndexWriteSet()->push_back(iwr1);

      Tuple partial_new_tuple =
          new_tuple.KeyFromTuple(table_info_->schema_, *(x->index_->GetKeySchema()), x->index_->GetKeyAttrs());
      x->index_->InsertEntry(partial_new_tuple, *rid, exec_ctx_->GetTransaction());

      auto iwr2 = IndexWriteRecord{*rid,          table_info_->oid_,      WType::INSERT, partial_new_tuple,
                                   x->index_oid_, exec_ctx_->GetCatalog()};
      //TODO  wir2 没写进去？
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
