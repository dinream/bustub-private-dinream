//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include <memory>
#include "catalog/schema.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  left_child_ = std::move(left_child);
  right_child_ = std::move(right_child);
  left_child_->GetOutputSchema();
  right_child_->GetOutputSchema();
  // std::cout << "Constructtttt \n" << std::endl;
  std::vector<Column> col;

  hjht_ = std::make_unique<HashJoinHashTable>(&(right_child_->GetOutputSchema()));
  // std::cout << "Constructtttt \n" << std::endl;
  plan_ = plan;

  iter_ = hjht_->Begin();
}

void HashJoinExecutor::Init() {
  // throw NotImplementedException("HashJoinExecutor is not implemented");
  // std::cout << "Init" << std::endl;
  left_child_->Init();
  right_child_->Init();
  auto l_schm = left_child_->GetOutputSchema();
  auto r_schm = right_child_->GetOutputSchema();
  std::vector<Column> columns;
  columns.insert(columns.end(), l_schm.GetColumns().begin(), l_schm.GetColumns().end());
  columns.insert(columns.end(), r_schm.GetColumns().begin(), r_schm.GetColumns().end());
  Schema schm(columns);
  Tuple tp{};
  RID rd{};
  // 插入 右 表

  while (right_child_->Next(&tp, &rd)) {
    // key
    JoinKey join_key_right;
    // std::cout<<"rrrrrrrrrrrrrrrrrrrtuple"<<std::endl;
    for (auto &exp : plan_->RightJoinKeyExpressions()) {
      join_key_right.j_keys_.emplace_back(exp->Evaluate(&tp, r_schm));
      // std::cout<<"rrrrrrrrrrrrrrrrrrr"<<join_key_right.j_keys_.size()<<std::endl;
    }
    // value
    std::vector<Value> values;
    for (size_t i = 0; i < r_schm.GetColumnCount(); i++) {
      values.push_back(tp.GetValue(&r_schm, i));
    }
    hjht_->InsertRight(join_key_right, values);
  }
  //
  // 插入 左 表
  while (left_child_->Next(&tp, &rd)) {
    // key
    JoinKey join_key_left;
    // std::cout<<"lllllllllllllllllllltuple"<<std::endl;
    for (auto &exp : plan_->LeftJoinKeyExpressions()) {
      // std::cout<<"llllllllllllllllllll"<<std::endl;
      join_key_left.j_keys_.emplace_back(exp->Evaluate(&tp, l_schm));
    }
    // value
    std::vector<Value> values;
    for (size_t i = 0; i < l_schm.GetColumnCount(); i++) {
      values.push_back(tp.GetValue(&l_schm, i));
    }
    hjht_->InsertLeft(join_key_left, values, schm, plan_->GetJoinType());
  }

  iter_ = hjht_->Begin();
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == hjht_->End()) {
    return false;
  }
  *tuple = *iter_;
  *rid = iter_->GetRid();
  iter_++;
  return true;
}
}  // namespace bustub
