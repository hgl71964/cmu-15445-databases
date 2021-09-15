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
#include "common/logger.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void NestIndexJoinExecutor::Init() {
  child_executor_->Init();
  i = 0;

  // populate
  auto inner_table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetInnerTableOid());
  auto inner_index_info = GetExecutorContext()->GetCatalog()->GetIndex(plan_->GetIndexName(), inner_table_info->name_);
  // LOG_INFO("outter %s", plan_->OuterTableSchema()->ToString().c_str());
  // LOG_INFO("inner %s", plan_->InnerTableSchema()->ToString().c_str());
  // LOG_INFO("child schema %s", child_executor_->GetOutputSchema()->ToString().c_str());
  // LOG_INFO("actual inner table %s", inner_table_info->schema_.ToString().c_str());
  // LOG_INFO("actual inner index %s", inner_index_info->key_schema_.ToString().c_str());

  // get all tuple from outter table
  try {
    Tuple tuple;
    RID rid;
    while (child_executor_->Next(&tuple, &rid)) {
      // make key
      std::vector<RID> rids;
      auto index_key = tuple.KeyFromTuple(*child_executor_->GetOutputSchema(), inner_index_info->key_schema_,
                                          inner_index_info->index_->GetKeyAttrs());
      inner_index_info->index_->ScanKey(index_key, &rids, GetExecutorContext()->GetTransaction());

      // if cannot find in index, then it does not match
      if (rids.empty()) {
        continue;
      }

      // get outter
      outter_table_tuple_.push_back(tuple);

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

bool NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) {
  // get schema
  const auto *out_schema = plan_->OuterTableSchema();
  const auto *inner_schema = plan_->InnerTableSchema();
  LOG_INFO("%ld %ld %ld", i, outter_table_tuple_.size(), inner_table_tuple_.size());

  // get join tuple
  while (i < outter_table_tuple_.size()) {
    // get tuples
    auto raw_left = outter_table_tuple_[i];
    auto raw_right = inner_table_tuple_[i];
    auto left = format_schema(&raw_left, child_executor_->GetOutputSchema(), out_schema);
    auto right = format_schema(
        &raw_right, &GetExecutorContext()->GetCatalog()->GetTable(plan_->GetInnerTableOid())->schema_, inner_schema);

    // incr
    i++;
    LOG_INFO("%ld %ld %ld", i, outter_table_tuple_.size(), inner_table_tuple_.size());

    // build tuple from left and right
    std::vector<Value> res;
    for (const Column &col : out_schema->GetColumns()) {
      Value val = left.GetValue(out_schema, out_schema->GetColIdx(col.GetName()));
      res.push_back(val);
    }
    for (const Column &col : inner_schema->GetColumns()) {
      Value val = right.GetValue(inner_schema, inner_schema->GetColIdx(col.GetName()));
      res.push_back(val);
    }
    *tuple = Tuple(res, GetOutputSchema());
    return true;
  }
  return false;
}

Tuple NestIndexJoinExecutor::format_schema(Tuple *tuple, const Schema *original_schema, const Schema *desire_schema) {
  std::vector<Value> res;
  for (const Column &col : desire_schema->GetColumns()) {
    Value val = tuple->GetValue(original_schema, original_schema->GetColIdx(col.GetName()));
    res.push_back(val);
  }
  return Tuple(res, desire_schema);
}

}  // namespace bustub
