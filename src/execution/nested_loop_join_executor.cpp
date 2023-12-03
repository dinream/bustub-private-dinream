//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "catalog/column.h"
#include "common/exception.h"
#include "common/rid.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  plan_ = plan;
  left_executor_ = std::move(left_executor);
  right_executor_ = std::move(right_executor);
}

void NestedLoopJoinExecutor::Init() {
  // throw NotImplementedException("NestedLoopJoinExecutor is not implemented");

  // left join 会返回左面所有的行
  // inner join 指挥返回匹配好的行
  // 用 deque 即可，左右比较比较用的 属性 的比较，没有特定的键
  left_executor_->Init();
  right_executor_->Init();

  std::vector<Tuple> left_tp;
  std::vector<Tuple> right_tp;
  Tuple tp{};
  RID rd{};
  while (left_executor_->Next(&tp, &rd)) {
    left_tp.push_back(tp);
  }
  while (right_executor_->Next(&tp, &rd)) {
    right_tp.push_back(tp);
  }
  // std::cout<<"len:"<<left_tp.size()<<"  "<<right_tp.size()<<std::endl;
  // 保存两个 指针 用于保存 这两个队列的 遍历位置
  auto l_iter = left_tp.begin();
  auto r_iter = right_tp.begin();
  // 保存临时左右 schema
  auto l_schm = left_executor_->GetOutputSchema();
  auto r_schm = right_executor_->GetOutputSchema();
  //
  // 构造一个新模式 schema
  std::vector<Column> columns;
  columns.insert(columns.end(), l_schm.GetColumns().begin(), l_schm.GetColumns().end());
  columns.insert(columns.end(), r_schm.GetColumns().begin(), r_schm.GetColumns().end());
  Schema schm(columns);
  while (l_iter != left_tp.end()) {
    auto l_match = false;
    std::vector<Value> values;
    while (r_iter != right_tp.end()) { // 如果右侧为 0 那么这里就进不去了
      // 到了此处， 左值一定存在
      bool match = false;  //  放在里面，允许左面的一行和右面的所行匹配
      auto haha = plan_->Predicate()->EvaluateJoin(&(*l_iter), left_executor_->GetOutputSchema(), &(*r_iter),
                                                   right_executor_->GetOutputSchema());
      if (!(haha.IsNull()) && haha.GetAs<bool>()) {  // 匹配成功
        match = true;
        // std::cout<<"匹配成功"<<std::endl;
      }
      
      switch (plan_->GetJoinType()) {
        case JoinType::LEFT:
          // 左连接，保留 左面的部分,, 但是需要注意只保留一次
          if (!match) {
            break;
          }
          l_match = true;
          for (size_t i = 0; i < l_schm.GetColumnCount(); i++) {
            values.push_back((*(l_iter)).GetValue(&l_schm, i));
          }
          for (size_t i = 0; i < r_schm.GetColumnCount(); i++) {
            values.push_back((*(r_iter)).GetValue(&r_schm, i));
          }
          result_.emplace_back(values, &schm);
          // std::cout<<"左连接"<<std::endl;
          break;
        case JoinType::INNER:
          if (!match) {
            break;
          }
          for (size_t i = 0; i < l_schm.GetColumnCount(); i++) {
            values.push_back((*(l_iter)).GetValue(&l_schm, i));
          }
          for (size_t i = 0; i < r_schm.GetColumnCount(); i++) {
            values.push_back((*(r_iter)).GetValue(&r_schm, i));
          }
          result_.emplace_back(values, &schm);
          // std::cout<<"内部连接"<<std::endl;
          break;
        case JoinType::INVALID:
        case JoinType::RIGHT:
        case JoinType::OUTER:
        default:
          break;
      }
      values.clear();
      r_iter++;
      // std::cout<<"内层循环"<<std::endl;
    }  // 内层
    if (r_iter == right_tp.end() && plan_->GetJoinType() == JoinType::LEFT) {
      if (!l_match) {
        for (size_t i = 0; i < l_schm.GetColumnCount(); i++) {
          values.push_back((*(l_iter)).GetValue(&l_schm, i));
        }
        for (size_t i = 0; i < r_schm.GetColumnCount(); i++) {
          values.push_back(ValueFactory::GetNullValueByType(r_schm.GetColumn(i).GetType()));
        }
        result_.emplace_back(values, &schm);
      }
    }
    r_iter = right_tp.begin();
    right_executor_->Init();
    while (right_executor_->Next(&tp, &rd)) {
    }
    l_iter++;
  }  // 无满足条件，返回 false ，外层
  iter_ = result_.begin();
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == result_.end()) {
    return false;
  }
  *tuple = *iter_;
  *rid = tuple->GetRid();
  ++iter_;
  return true;
}

}  // namespace bustub
