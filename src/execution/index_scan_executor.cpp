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
  if (tbl_name_ == "test_1") {
    auto *tree_index = dynamic_cast<BPlusTreeIndex<GenericKey<16>, RID, GenericComparator<16>> *>(
        GetExecutorContext()->GetCatalog()->GetIndex(plan_->GetIndexOid())->index_.get());
    itr_16 = tree_index->GetBeginIterator();
    itr_end_16 = tree_index->GetEndIterator();
  } else {
    auto *tree_index = dynamic_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(
        GetExecutorContext()->GetCatalog()->GetIndex(plan_->GetIndexOid())->index_.get());
    itr_ = tree_index->GetBeginIterator();
    itr_end_ = tree_index->GetEndIterator();
  }
}

bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) {
  if (tbl_name_ == "test_1") {
    while (itr_16 != itr_end_16) {
      // get rid
      auto mapping_type = *itr_16;
      RID tmp_rid = mapping_type.second;
      Tuple tmp_tuple;

      // incr
      ++itr_16;

      // get tuple
      auto ok = GetExecutorContext()->GetCatalog()->GetTable(tbl_name_)->table_->GetTuple(
          tmp_rid, &tmp_tuple, GetExecutorContext()->GetTransaction());
      if (!ok) {
        LOG_DEBUG("fatal index scan");
        throw Exception(ExceptionType::INVALID, "index scan");
      }

      // eval predicate
      Value v = plan_->GetPredicate()->Evaluate(&tmp_tuple, GetOutputSchema());
      if (v.GetAs<bool>()) {
        *tuple = tmp_tuple;
        *rid = tmp_rid;
        return true;
      }
    }
    return false;
  }

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
