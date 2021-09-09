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

    // incr
    ++itr_;

    // eval predicate
    Value v = plan_->GetPredicate()->Evaluate(&tmp_tuple, GetOutputSchema());
    if (v.GetAs<bool>()) {
      *tuple = tmp_tuple;
      //LOG_DEBUG("%d %d %d %p", tuple->GetValue(GetOutputSchema(), GetOutputSchema()->GetColIdx("colA")).GetAs<int32_t>(),
      //          tuple->GetValue(GetOutputSchema(), GetOutputSchema()->GetColIdx("colB")).GetAs<int32_t>(),
      //          v.GetAs<bool>(), 
      //          tuple);
      *rid = itr_->GetRid();
      return true;
    }
  }
  return false;
}

}  // namespace bustub
