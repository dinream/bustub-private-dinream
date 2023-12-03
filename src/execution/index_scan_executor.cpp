//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
}

void IndexScanExecutor::Init() {
  // table，index，所以需要一个table iterator
  auto des_index_id = plan_->index_oid_;
  auto des_index_info = exec_ctx_->GetCatalog()->GetIndex(des_index_id);
  b_tree_index_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(des_index_info->index_.get());
  // 保存一个迭代器
  iter_ = b_tree_index_->GetBeginIterator();

  tableinfo_ = exec_ctx_->GetCatalog()->GetTable(des_index_info->table_name_);
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 在一个表上进行索引扫描？
  if (iter_.IsEnd()) {
    return false;
  }
  *tuple = tableinfo_->table_->GetTuple((*iter_).second).second;
  *rid = tuple->GetRid();
  ++iter_;
  return true;
}
}  // namespace bustub
