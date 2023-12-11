//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// sort_executor.h
//
// Identification: src/include/execution/executors/sort_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

#include "catalog/schema.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/sort_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
class Mycomp1 {  // 降序
 public:
  Mycomp1(const std::shared_ptr<AbstractExpression> &ob_expr, const Schema *schm) : schm_(*schm) {
    ob_expr_ = ob_expr.get();
    schm_ = *schm;
  }
  AbstractExpression *ob_expr_;
  Schema schm_;

  auto operator()(const Tuple &tp1, const Tuple &tp2) -> bool {
    auto val1 = (ob_expr_)->Evaluate(&tp1, schm_);
    auto val2 = (ob_expr_)->Evaluate(&tp2, schm_);
    return val1.CompareGreaterThan(val2) == CmpBool::CmpTrue;
  }
};
class Mycomp2 {  // 降序
 public:
  Mycomp2(const std::shared_ptr<AbstractExpression> &ob_expr, const Schema *schm) : schm_(*schm) {
    std::cout << "hqa" << std::endl;
    ob_expr_ = ob_expr.get();
    std::cout << "wwowoo" << std::endl;

    schm_ = *schm;
    std::cout << "hqa11" << std::endl;
  }
  AbstractExpression *ob_expr_;
  Schema schm_;

  auto operator()(const Tuple &tp1, const Tuple &tp2) -> bool {
    auto val1 = (ob_expr_)->Evaluate(&tp1, schm_);
    auto val2 = (ob_expr_)->Evaluate(&tp2, schm_);
    return val1.CompareLessThanEquals(val2) == CmpBool::CmpTrue;
  }
};

/**
 * The SortExecutor executor executes a sort.
 */
class SortExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new SortExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The sort plan to be executed
   */
  SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the sort */
  void Init() override;

  /**
   * Yield the next tuple from the sort.
   * @param[out] tuple The next tuple produced by the sort
   * @param[out] rid The next tuple RID produced by the sort
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the sort */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }
  auto static CoMP(const Tuple &tp1, const Tuple &tp2) -> bool;
  // auto static Comp(const Tuple &tp1,const Tuple &tp2)  -> bool;
  // static std::shared_ptr<AbstractExpression> ob_expr;
  // static Schema schm;
 private:
  /** The sort plan node to be executed */
  const SortPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> child_executor_;
  std::vector<Tuple> tuples_;
  size_t ptr_;
};
}  // namespace bustub
