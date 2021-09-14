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

#include "common/logger.h"
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

  LOG_INFO("AggregationExecutor %ld ", child_vec_.size());

  // populate hash table
  for (auto &tpl : child_vec_) {
    auto *p = &tpl;
    auto key = MakeKey(p);
    auto val = MakeVal(p);
    aht_.InsertCombine(key, val);
  }
  aht_iterator_ = aht_.Begin();  // update itr
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  auto end = aht_.End();
  while (aht_iterator_ != end) {
    auto key = aht_iterator_.Key();
    auto val = aht_iterator_.Val();
    ++aht_iterator_;

    auto *having = plan_->GetHaving();
    if (having == nullptr || having->EvaluateAggregate(key.group_bys_, val.aggregates_).GetAs<bool>()) {
      // LOG_INFO("%ld %ld %d", key.group_bys_.size(), val.aggregates_.size(), GetOutputSchema()->GetColumnCount());
      auto v1 = key.group_bys_;
      auto v2 = val.aggregates_;
      v1.insert(v1.end(), v2.begin(), v2.end());
      *tuple = Tuple(v1, GetOutputSchema());
      return true;
    }
  }
  return false;
}

}  // namespace bustub
