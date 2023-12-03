//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <utility>

#include "execution/executors/delete_executor.h"
#include "type/type_id.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
}

void DeleteExecutor::Init() {
  // throw NotImplementedException("DeleteExecutor is not implemented");
  is_ok_ = false;
  child_executor_->Init();
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (is_ok_) {
    return false;
  }
  // BufferPoolManager *bpm_=exec_ctx_->GetBufferPoolManager();
  Catalog *cl = exec_ctx_->GetCatalog();
  auto &t_heap = cl->GetTable(plan_->TableOid())->table_;
  auto tableinfo = cl->GetTable(plan_->TableOid());
  auto indexsinfo = cl->GetTableIndexes(tableinfo->name_);
  int n = 0;                                   // 计数
  while (child_executor_->Next(tuple, rid)) {  // 到了这里，删除算子不包括 迭代了，拿到的就是要删除的值
    // 确定要更新的目标元组，可能有多个
    n++;
    auto tuplemeta = t_heap->GetTupleMeta(*rid);
    tuplemeta.is_deleted_ = true;
    t_heap->UpdateTupleMeta(tuplemeta, *rid);

    auto twr = TableWriteRecord{tableinfo->oid_, *rid, tableinfo->table_.get()};
    twr.wtype_ = WType::DELETE;
    exec_ctx_->GetTransaction()->GetWriteSet()->push_back(twr);

    for (auto &x : indexsinfo) {  // 每个索引
      Tuple new_tuple = tuple->KeyFromTuple(tableinfo->schema_, *(x->index_->GetKeySchema()), x->index_->GetKeyAttrs());
      x->index_->DeleteEntry(new_tuple, *rid, exec_ctx_->GetTransaction());
      auto iwr =
          IndexWriteRecord{*rid, tableinfo->oid_, WType::DELETE, new_tuple, x->index_oid_, exec_ctx_->GetCatalog()};
      exec_ctx_->GetTransaction()->GetIndexWriteSet()->push_back(iwr);
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
