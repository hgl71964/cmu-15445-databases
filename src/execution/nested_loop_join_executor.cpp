//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_(std::move(left_executor)),
      right_(std::move(right_executor)),
      populated_(false) {}

void NestedLoopJoinExecutor::Init() {
  left_->Init();
  right_->Init();
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (!populated_) {
    populate();
  }

  while (i < left_set_.size()) {
    while (j < right_set_.size()) {
      // get tuples
      auto left = left_set_[i];
      auto right = right_set_[j];

      // incr
      j++;

      // verify
      auto *p = plan_->Predicate();
      if (p == nullptr ||
          p->EvaluateJoin(&left, left_->GetOutputSchema(), &right, right_->GetOutputSchema()).GetAs<bool>()) {
        // build tuple from left and right
        std::vector<Value> res;
        for (const Column &col : left_->GetOutputSchema()->GetColumns()) {
          Value val = left.GetValue(left_->GetOutputSchema(), left_->GetOutputSchema()->GetColIdx(col.GetName()));
          res.push_back(val);
        }
        for (const Column &col : right_->GetOutputSchema()->GetColumns()) {
          Value val = right.GetValue(right_->GetOutputSchema(), right_->GetOutputSchema()->GetColIdx(col.GetName()));
          res.push_back(val);
        }
        Tuple new_tuple(res, GetOutputSchema());
        *tuple = new_tuple;
        return true;
      }
    }
    j = 0;
    i++;
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
