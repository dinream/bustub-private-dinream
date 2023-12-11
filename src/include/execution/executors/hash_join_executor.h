//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstddef>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>
#include "binder/table_ref/bound_join_ref.h"
#include "catalog/schema.h"
#include "common/util/hash_util.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {
// 此类 用于当作 hashtable 的 键
struct JoinKey {
  JoinKey() = default;
  std::vector<Value> j_keys_;
  auto operator==(const JoinKey &other) const -> bool {
    // std::cout<<"o dui"<<std::endl;
    for (uint32_t i = 0; i < other.j_keys_.size(); i++) {
      if (j_keys_[i].CompareEquals(other.j_keys_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

// 此类用于 JoinHashTable 的 值
struct JoinValue {
  JoinValue() = default;
  std::vector<std::vector<Value>> j_values_{};
};
}  // namespace bustub

namespace std {

/** Implements std::hash on JoinKey */
template <>
struct hash<bustub::JoinKey> {
  auto operator()(const bustub::JoinKey &agg_key) const -> std::size_t {
    // std::cout<<"a zhe "<<std::endl;
    size_t curr_hash = 0;
    for (const auto &key : agg_key.j_keys_) {
      if (!key.IsNull()) {
        curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
      }
    }
    // std::cout<<"cur hash:"<<curr_hash<<std::endl;
    return curr_hash;
  }
};
}  // namespace std

namespace bustub {

class HashJoinHashTable {
 public:
  explicit HashJoinHashTable(const Schema *r_schm) {
    // std::cout<<"gouzao "<<std::endl;

    for (size_t i = 0; i < r_schm->GetColumnCount(); i++) {
      null_values_.push_back(ValueFactory::GetNullValueByType(r_schm->GetColumn(i).GetType()));
    }
  }

  void InsertLeft(const JoinKey &join_key, std::vector<Value> &left_values, Schema schm,
                  bustub::JoinType jt) {  // 插入的同时 插入 ht_
    // std::cout<<"count begin "<<std::endl;
    size_t n = tmp_ht_.count(join_key);
    // std::cout<<"count end "<<std::endl;
    // std:: cout<<n<<std::endl;
    if (n > 0) {
      // 已经存在了，匹配成功
      // 构造一个新的 Tuple 保存下来

      for (auto &j_value : tmp_ht_[join_key].j_values_) {
        std::vector<Value> values;
        values.insert(values.end(), left_values.begin(), left_values.end());
        values.insert(values.end(), j_value.begin(), j_value.end());
        ht_.emplace_back(values, &schm);  // 插入一个 tuple
      }
    } else if (jt == JoinType::LEFT) {
      std::vector<Value> values;
      values.insert(values.end(), left_values.begin(), left_values.end());
      values.insert(values.end(), null_values_.begin(), null_values_.end());
      ht_.emplace_back(values, &schm);
    }
  }

  void InsertRight(const JoinKey &join_key, std::vector<Value> &right_values) {  // 插入中间态
    //
    if (tmp_ht_.count(join_key) == 0) {
      bustub::JoinValue j_values;
      j_values.j_values_.emplace_back(right_values);  // 将vector 插入 vector
      tmp_ht_[join_key] = j_values;                   // 插入
    } else {
      tmp_ht_[join_key].j_values_.emplace_back(right_values);  // 追加到 末尾
    }
    // return
  }
  auto Begin() -> std::vector<Tuple>::iterator { return ht_.begin(); }
  auto End() -> std::vector<Tuple>::iterator { return ht_.end(); }
  void Clear() { ht_.clear(); }

 private:
  // 保存 hashtable 键值对 值 最好 是 最终的元组，否则  也可以 是 values
  std::vector<Tuple> ht_{};
  // 需要一个 中间件，先保存右侧 值，
  std::unordered_map<JoinKey, JoinValue> tmp_ht_{};
  // 保存空值
  std::vector<Value> null_values_;
  // 保存左右模式
  // const Schema* l_schm_;
  // const Schema* r_schm_;
  // 保存 输出模式
};
// hashtable 的值 是 使用的 元组，
/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join.
   * @param[out] rid The next tuple RID, not used by hash join.
   * @return `true` if a tuple was produced, `false` if there are no more tuples.
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_child_;
  std::unique_ptr<AbstractExecutor> right_child_;
  /** Simple aggregation hash table */
  std::unique_ptr<HashJoinHashTable> hjht_;
  /** Simple aggregation hash table iterator */
  std::vector<Tuple>::iterator iter_;
};

}  // namespace bustub
