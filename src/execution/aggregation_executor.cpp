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
#include "execution/expressions/column_value_expression.h"

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
      auto key = MakeKey(&tuple);
      auto val = MakeVal(&tuple);
      aht_.InsertCombine(key, val);
    }
  } catch (Exception &e) {
    LOG_DEBUG("AggregationExecutor %s", e.what());
  }
  aht_iterator_ = aht_.Begin();  // update itr
}

std::vector<Value> AggregationExecutor::resemble(const std::vector<Value> &v1, const std::vector<Value> &v2) {
  std::vector<Value> res;
  for (const Column &col : GetOutputSchema()->GetColumns()) {
    const AbstractExpression *expr = col.GetExpr();  // this is aggregate expression
    auto val = expr->EvaluateAggregate(v1, v2);
    res.push_back(val);
  }
  return res;
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  while (aht_iterator_ != aht_.End()) {
    auto key = aht_iterator_.Key();
    auto val = aht_iterator_.Val();
    ++aht_iterator_;

    auto *having = plan_->GetHaving();
    if (having == nullptr || having->EvaluateAggregate(key.group_bys_, val.aggregates_).GetAs<bool>()) {
      LOG_INFO("%ld %ld %d", key.group_bys_.size(), val.aggregates_.size(), GetOutputSchema()->GetColumnCount());
      LOG_INFO("%s", GetOutputSchema()->ToString().c_str());
      auto res = resemble(key.group_bys_, val.aggregates_);

      *tuple = Tuple(res, GetOutputSchema());
      return true;
    }
  }
  return false;
}

}  // namespace bustub
