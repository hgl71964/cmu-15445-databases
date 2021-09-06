//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() {
  limit_cnt_ = 0;
  offset_cnt_ = 0;
}

bool LimitExecutor::Next(Tuple *tuple, RID *rid) {
  if (limit_cnt_ >= plan_->GetLimit()) {
    return false;
  }

  while (child_executor_->Next(tuple, rid)) {
    if (offset_cnt_ < plan_->GetOffset()) {
      offset_cnt_++;
      continue;
    }
    if (limit_cnt_ < plan_->GetLimit()) {
      limit_cnt_++;
      return true;
    }
  }
  return false;
}

}  // namespace bustub
