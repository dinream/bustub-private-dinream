#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
  plan_ = plan;
  child_executor_ = std::move(child_executor);
}

void SortExecutor::Init() {
  child_executor_->Init();

  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    tuples_.push_back(tuple);
  }

  auto order_bys = plan_->GetOrderBy();

  std::sort(tuples_.begin(), tuples_.end(), [&](const Tuple &a, const Tuple &b) {
    for (auto &x : order_bys) {
      auto express = x.second; // 这里没有直接转为 Column，是因为可能是 Column 的表达式
      auto a_val = express->Evaluate(&a, plan_->GetChildPlan()->OutputSchema());
      auto b_val = express->Evaluate(&b, plan_->GetChildPlan()->OutputSchema());

      auto ordertype = x.first;
      if (a_val.CompareLessThan(b_val) == CmpBool::CmpTrue) {
        return ordertype == OrderByType::ASC || ordertype == OrderByType::DEFAULT;
      }
      if (a_val.CompareGreaterThan(b_val) == CmpBool::CmpTrue) {
        return ordertype == OrderByType::DESC;
      }
      // 如果是 equal 进行下一个比较
    }

    return true;
  });

  ptr_ = 0;
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (ptr_ == tuples_.size()) {
    return false;
  }

  *tuple = tuples_[ptr_];
  *rid = tuple->GetRid();
  ptr_++;

  return true;
}

}  // namespace bustub
