//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      aht_(SimpleAggregationHashTable{plan->GetAggregates(), plan->GetAggregateTypes()}),
      aht_iterator_(aht_.Begin()) {
  plan_ = plan;
  child_ = std::move(child);
  cnt_ = 0;
  has_out_ = false;
}

void AggregationExecutor::Init() {
  child_->Init();  // 初始化孩子节点
  // auto cl = exec_ctx_->GetCatalog(); // 数据库 表 目录
  aht_.Clear();
  Tuple tp = Tuple{};
  RID rid = RID();
  while (child_->Next(&tp, &rid)) {
    cnt_++;
    aht_.InsertCombine(MakeAggregateKey(&tp), MakeAggregateValue(&tp));  // 把孩子的每一个 tuple 都插入进去
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 构造一个 AggregationExectutor  迭代器，遍历一个 group by 列，
  // 应该是对每一个 key 都 用value 聚合一下
  if (cnt_ == 0 && !has_out_) {
    if (!plan_->GetGroupBys().empty()) {
      return false;
    }

    // empty db case
    std::vector<Value> val;
    auto aggregate_types = plan_->GetAggregateTypes();
    for (size_t i = 0; i < plan_->GetAggregates().size(); i++) {
      if (aggregate_types[i] == AggregationType::CountStarAggregate) {
        val.push_back(ValueFactory::GetIntegerValue(0));
      } else {
        val.push_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
      }
    }

    *tuple = Tuple(val, &GetOutputSchema());

    has_out_ = true;
    return true;
  }

  if (aht_iterator_ == aht_.End()) {
    return false;
  }

  std::vector<Value> val;
  for (auto const &x : aht_iterator_.Key().group_bys_) {
    val.push_back(x);
  }
  for (auto const &x : aht_iterator_.Val().aggregates_) {
    val.push_back(x);
  }

  *tuple = Tuple(val, &GetOutputSchema());

  ++aht_iterator_;

  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
