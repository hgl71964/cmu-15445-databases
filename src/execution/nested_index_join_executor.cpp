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

  while (i < outter_table_tuple_.size()) {
    while (j < inner_table_tuple_.size()) {
      // get tuples
      auto left = outter_table_tuple_[i];
      auto right = inner_table_tuple_[j];

      // get inner schema
      auto schema = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetInnerTableOid())->schema_;

      // incr
      j++;

      // verify
      auto *p = plan_->Predicate();
      if (p == nullptr || p->EvaluateJoin(&left, child_executor_->GetOutputSchema(), &right, nullptr).GetAs<bool>()) {
        // build tuple from left and right
        std::vector<Value> res;
        for (const Column &col : child_executor_->GetOutputSchema()->GetColumns()) {
          Value val = left.GetValue(child_executor_->GetOutputSchema(),
                                    child_executor_->GetOutputSchema()->GetColIdx(col.GetName()));
          res.push_back(val);
        }
        for (const Column &col : schema.GetColumns()) {
          Value val = right.GetValue(&schema, schema.GetColIdx(col.GetName()));
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

void NestIndexJoinExecutor::populate() {
  populated_ = true;
  i = 0;
  j = 0;

  auto inner_table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetInnerTableOid());
  auto inner_index_info = GetExecutorContext()->GetCatalog()->GetIndex(plan_->GetIndexName(), inner_table_info->name_);

  // get all tuple from outter table
  std::vector<Tuple> outter_table_tuple_;
  try {
    Tuple tuple;
    RID rid;
    while (child_executor_->Next(&tuple, &rid)) {
      outter_table_tuple_.push_back(tuple);
    }
  } catch (Exception &e) {
    LOG_DEBUG("NestIndexJoinExecutor %s", e.what());
  }

  std::vector<Tuple> inner_table_tuple_;
  std::vector<RID> rids;
  for (auto &tpl : outter_table_tuple_) {
    rids.clear();
    auto index_key = tpl.KeyFromTuple(inner_table_info->schema_, inner_index_info->key_schema_,
                                      inner_index_info->index_->GetKeyAttrs());
    inner_index_info->index_->ScanKey(index_key, &rids, GetExecutorContext()->GetTransaction());

    // get tuple
    Tuple inner_tuple;
    inner_table_info->table_->GetTuple(rids[0], &inner_tuple, GetExecutorContext()->GetTransaction());
    inner_table_tuple_.push_back(inner_tuple);
  }
}

}  // namespace bustub
