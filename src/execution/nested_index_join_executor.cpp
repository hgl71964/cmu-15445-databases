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

  // get inner schema
  const auto *out_schema = plan_->OuterTableSchema();
  const auto *inner_schema = plan_->InnerTableSchema();

  while (i < outter_table_tuple_.size()) {
    while (j < inner_table_tuple_.size()) {
      // get tuples
      auto raw_left = outter_table_tuple_[i];
      auto raw_right = inner_table_tuple_[j];
      auto left = format_schema(&raw_left, out_schema);
      auto right = format_schema(&raw_right, inner_schema);

      // incr
      j++;

      // verify
      auto *p = plan_->Predicate();
      if (p == nullptr || p->EvaluateJoin(&left, out_schema, &right, inner_schema).GetAs<bool>()) {
        // build tuple from left and right
        std::vector<Value> res;
        for (const Column &col : out_schema->GetColumns()) {
          Value val = left.GetValue(out_schema, out_schema->GetColIdx(col.GetName()));
          res.push_back(val);
        }
        LOG_INFO("outter %ld", res.size());
        for (const Column &col : inner_schema->GetColumns()) {
          Value val = right.GetValue(inner_schema, inner_schema->GetColIdx(col.GetName()));
          res.push_back(val);
        }
        LOG_INFO("inner %ld", res.size());

        *tuple = Tuple(res, GetOutputSchema());
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
  std::vector<Tuple> inner_table_tuple_;
  std::vector<RID> rids;
  auto *out_schema = plan_->OuterTableSchema();
  try {
    Tuple tuple;
    RID rid;
    while (child_executor_->Next(&tuple, &rid)) {
      // get outter
      outter_table_tuple_.push_back(tuple);

      // make key
      rids.clear();
      auto index_key =
          tuple.KeyFromTuple(*out_schema, inner_index_info->key_schema_, inner_index_info->index_->GetKeyAttrs());
      inner_index_info->index_->ScanKey(index_key, &rids, GetExecutorContext()->GetTransaction());

      // get inner tuple
      Tuple inner_tuple;
      inner_table_info->table_->GetTuple(rids[0], &inner_tuple, GetExecutorContext()->GetTransaction());
      inner_table_tuple_.push_back(inner_tuple);
    }
  } catch (Exception &e) {
    LOG_DEBUG("NestIndexJoinExecutor %s", e.what());
  }

  LOG_INFO("outter tuple: %ld - inner tuple: %ld", outter_table_tuple_.size(), inner_table_tuple_.size());
}

Tuple NestIndexJoinExecutor::format_schema(Tuple *tuple, const Schema *schema) {
  std::vector<Value> res;
  for (const Column &col : schema->GetColumns()) {
    Value val = tuple->GetValue(schema, schema->GetColIdx(col.GetName()));
    res.push_back(val);
  }
  return Tuple(res, schema);
}

}  // namespace bustub
