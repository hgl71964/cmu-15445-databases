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
}

bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (itr_ != itr_end_) {
    // get rid
    auto mapping_type = *itr_;
    RID tmp_rid = mapping_type.second;
    Tuple tmp_tuple;

    // incr
    ++itr_;

    // get tuple
    auto ok = GetExecutorContext()->GetCatalog()->GetTable(tbl_name_)->table_->GetTuple(
        tmp_rid, &tmp_tuple, GetExecutorContext()->GetTransaction());
    if (!ok) {
      LOG_DEBUG("fatal index scan");
      throw Exception(ExceptionType::INVALID, "index scan");
    }
    auto index_key = tmp_tuple.KeyFromTuple(
        GetExecutorContext()->GetCatalog()->GetTable(tbl_name_)->schema_,
        GetExecutorContext()->GetCatalog()->GetIndex(plan_->GetIndexOid())->key_schema_,
        GetExecutorContext()->GetCatalog()->GetIndex(plan_->GetIndexOid())->index_->GetKeyAttrs());

    // eval predicate
    auto *p = plan_->GetPredicate();  // could be nullptr
    if (p == nullptr || plan_->GetPredicate()->Evaluate(&index_key, GetOutputSchema()).GetAs<bool>()) {
      *tuple = index_key;
      *rid = tmp_rid;  // XXX rid?
      return true;
    }
  }
  return false;
}

}  // namespace bustub
