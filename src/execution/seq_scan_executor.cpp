//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"
#include "common/logger.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid())),
      rid_(),
      done(false) {}

void SeqScanExecutor::Init() {
  auto itr = table_info_->table_->Begin(GetExecutorContext()->GetTransaction());
  rid_ = itr->GetRid();
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  if (done) {
    return false;
  }
  auto itr = TableIterator(table_info_->table_.get(), rid_, GetExecutorContext()->GetTransaction());
  while (itr != table_info_->table_->End()) {
    // get tuple
    auto tmp_tuple = *itr;

    // format output tuple
    const Schema *schema = GetOutputSchema();
    std::vector<Value> res;
    for (const Column &col : schema->GetColumns()) {
      Value val = tmp_tuple.GetValue(schema, schema->GetColIdx(col.GetName()));
      res.push_back(val);
    }
    Tuple new_tuple(res, schema);

    // incr
    ++itr;
    if (itr == table_info_->table_->End()) {
      done = true;
    } else {
      rid_ = itr->GetRid();
    }

    // eval predicate
    auto *p = plan_->GetPredicate();  // could be nullptr
    if (p == nullptr || plan_->GetPredicate()->Evaluate(&new_tuple, schema).GetAs<bool>()) {
      *tuple = new_tuple;
      *rid = tmp_tuple.GetRid();  // XXX seems ok, outputSchema is changed but RID is the same
      return true;
    }
  }
  return false;
}

}  // namespace bustub
