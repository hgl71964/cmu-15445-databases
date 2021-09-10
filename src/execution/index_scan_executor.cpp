//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include "common/exception.h"
#include "common/logger.h"

namespace bustub {

IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      tbl_name_(GetExecutorContext()->GetCatalog()->GetIndex(plan_->GetIndexOid())->table_name_) {}

void IndexScanExecutor::Init() {
  auto *tree_index = dynamic_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(
      GetExecutorContext()->GetCatalog()->GetIndex(plan_->GetIndexOid())->index_.get());
  itr_ = tree_index->GetBeginIterator();
  itr_end_ = tree_index->GetEndIterator();
  // itr_(GetExecutorContext()->GetCatalog()->GetIndex(plan_->GetIndexOid())->index_->GetBeginIterator()),
  // itr_end_(GetExecutorContext()->GetCatalog()->GetIndex(plan_->GetIndexOid())->index_->GetEndIterator()),
}

bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) {
  LOG_INFO("INDEX SCAN");

  while (itr_ != itr_end_) {
    // get rid
    auto mapping_type = *itr_;
    RID tmp_rid = mapping_type.second;
    Tuple tmp_tuple;

    // get tuple
    auto ok = GetExecutorContext()->GetCatalog()->GetTable(tbl_name_)->table_->GetTuple(
        tmp_rid, &tmp_tuple, GetExecutorContext()->GetTransaction());
    if (!ok) {
      LOG_DEBUG("fatal index scan");
      throw Exception(ExceptionType::INVALID, "index scan");
    }

    // incr
    ++itr_;

    // eval predicate
    Value v = plan_->GetPredicate()->Evaluate(&tmp_tuple, GetOutputSchema());
    if (v.GetAs<bool>()) {
      *tuple = tmp_tuple;
      // LOG_DEBUG("%d %d %d %p", tuple->GetValue(GetOutputSchema(),
      // GetOutputSchema()->GetColIdx("colA")).GetAs<int32_t>(),
      //          tuple->GetValue(GetOutputSchema(), GetOutputSchema()->GetColIdx("colB")).GetAs<int32_t>(),
      //          v.GetAs<bool>(),
      //          tuple);
      *rid = tmp_rid;
      return true;
    }
  }
  return false;
}

}  // namespace bustub
