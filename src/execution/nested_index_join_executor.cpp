//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)), populated_(false) {}

void NestIndexJoinExecutor::Init() { child_executor_->Init(); }

bool NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (!populated_) {
    populate();
  }
  return false;
}

void NestedLoopJoinExecutor::populate() {
  populated_ = true;
  i = 0;
  j = 0;

  try {
    Tuple tuple;
    RID rid;
    while (left_->Next(&tuple, &rid)) {
      left_set_.push_back(tuple);
    }
  } catch (Exception &e) {
    LOG_DEBUG("NestedLoopJoinExecutor %s", e.what());
  }

  try {
    Tuple tuple;
    RID rid;
    while (right_->Next(&tuple, &rid)) {
      right_set_.push_back(tuple);
    }
  } catch (Exception &e) {
    LOG_DEBUG("NestedLoopJoinExecutor %s", e.what());
  }
}

}  // namespace bustub
