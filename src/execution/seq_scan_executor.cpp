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
#include "buffer/buffer_pool_manager.h"
#include "catalog/catalog.h"
#include "common/macros.h"
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
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  // throw NotImplementedException("SeqScanExecutor is not implemented");
  Catalog *cl = exec_ctx_->GetCatalog();
  t_id_= plan_->GetTableOid();
  TableInfo *table = cl->GetTable(plan_->GetTableOid());
  // BUSTUB_ASSERT(table == nullptr, "table is not exist ");
  t_iter_ = new TableIterator(table->table_->MakeIterator());
  // t_iter_ = TableIterator((table->table_), RID{INVALID_PAGE_ID, 0}, RID{INVALID_PAGE_ID, 0});
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {  // 主要是为了防止 被删除的元组连续出现
    if (t_iter_->IsEnd()) { 
      if(t_iter_!= nullptr){ // 防止多次 Next 导致多次释放
        delete t_iter_;
        t_iter_ = nullptr;
      }
      return false;
    }
    // std::cout << "hah" << std::endl;
    if (t_iter_->GetTuple().first.is_deleted_ || 
            (plan_->filter_predicate_ != nullptr && 
            plan_->filter_predicate_->Evaluate(tuple, exec_ctx_->GetCatalog()->GetTable(t_id_)->schema_)
                .CompareEquals(ValueFactory::GetBooleanValue(false)) == CmpBool::CmpTrue)) { // 出现 where 还需要比较一下
      ++(*t_iter_);  // 下移
      continue;
    }
    *tuple = t_iter_->GetTuple().second;
    *rid = t_iter_->GetRID();
    break;
  }
  ++(*t_iter_);
  // return !((++t_iter_)->IsEnd()); 只是要返回了 数据 ，说明 就是 false
  return true;
}

}  // namespace bustub
