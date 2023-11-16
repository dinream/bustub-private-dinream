//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "catalog/catalog.h"
#include "execution/executors/insert_executor.h"
#include "storage/table/table_iterator.h"
#include "storage/table/tuple.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx) {
    plan_=plan;
    child_executor_=child_executor.get();
}

void InsertExecutor::Init() { 
    // throw NotImplementedException("InsertExecutor is not implemented"); 
    
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { 
    // BufferPoolManager *bpm_=exec_ctx_->GetBufferPoolManager();
    Catalog* cl = exec_ctx_->GetCatalog();
    auto &t_heap=cl->GetTable(plan_->TableOid())->table_;
    auto table= cl->GetTable(plan_->TableOid());
    // TableIterator t_iter=cl->GetTable(plan_->TableOid())->table_->MakeIterator();  虽然有可以进行插入的tableheap，但是这里的 t_iter 中无法访问到  tableheap，并且远了
    
    Tuple tp;
    RID ri{};
    // 一个计划节点中会不会有多个值
    while(child_executor_->Next(&tp, &ri)){ 
        // 虽然说 t_heap->GetTupleMeta(ri) 这里可以得到 tupleMeta， 但是 ，这里得重新构造一个,
        
        t_heap->InsertTuple({0,0,false}, tp);
    }
    //t_heap->InsertTuple(const TupleMeta &mta, const Tuple &tule)
    return true; // child_executor_->Next(&tp, &ri) 神么情况下返回false；
}

}  // namespace bustub
