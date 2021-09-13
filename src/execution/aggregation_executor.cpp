//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

void AggregationExecutor::Init() {
  // get tuples from child
  child_->Init();
  try {
    Tuple tuple;
    RID rid;
    while (child_->Next(&tuple, &rid)) {
      child_vec_.push_back(tuple);
    }
  } catch (Exception &e) {
    LOG_DEBUG("AggregationExecutor %s", e.what());
  }
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) { return false; }

}  // namespace bustub
