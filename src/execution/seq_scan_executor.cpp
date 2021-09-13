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
      itr_(GetExecutorContext()
               ->GetCatalog()
               ->GetTable(plan_->GetTableOid())
               ->table_->Begin(GetExecutorContext()->GetTransaction())),
      itr_end_(GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid())->table_->End()) {
  // auto tbl_oid = plan_->GetTableOid();
  // auto *tbl_meta = GetExecutorContext()->GetCatalog()->GetTable(tbl_oid);
  // TableIterator itr = tbl_meta->table_->Begin(GetExecutorContext()->GetTransaction());
  // TableIterator itr_end = tbl_meta->table_->End();
}

void SeqScanExecutor::Init() {
  // auto *tbl_meta = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid());
  // LOG_DEBUG("name %s %d", tbl_meta->name_.c_str(), tbl_meta->oid_);
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (itr_ != itr_end_) {
    // get tuple
    auto tmp_tuple = *itr_;
    std::vector<Value> res;
    for (const Column &col : GetOutputSchema()->GetColumns()) {
      Value val = tmp_tuple.GetValue(GetOutputSchema(), GetOutputSchema()->GetColIdx(col.GetName()));
      res.push_back(val);
    }
    Tuple new_tuple(res, GetOutputSchema());

    // incr
    ++itr_;

    // eval predicate
    auto *p = plan_->GetPredicate();  // could be nullptr
    if (p == nullptr || plan_->GetPredicate()->Evaluate(&new_tuple, GetOutputSchema()).GetAs<bool>()) {
      *tuple = new_tuple;
      *rid = tmp_tuple.GetRid();  // XXX seems ok, outputSchema is changed but RID is the same
      return true;
    }
  }
  return false;
}

}  // namespace bustub
