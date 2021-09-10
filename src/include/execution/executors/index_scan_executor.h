//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.h
//
// Identification: src/include/execution/executors/index_scan_executor.h
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <vector>

#include "common/rid.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/index_scan_plan.h"
#include "storage/table/tuple.h"
// #include "storage/index/b_plus_tree_index.h"
// #include "storage/page/b_plus_tree_page.h"

namespace bustub {

/**
 * IndexScanExecutor executes an index scan over a table.
 */
class IndexScanExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new index scan executor.
   * @param exec_ctx the executor context
   * @param plan the index scan plan to be executed
   */
  IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan);

  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

  void Init() override;

  bool Next(Tuple *tuple, RID *rid) override;

 private:
  /** The index scan plan node to be executed. */
  const IndexScanPlanNode *plan_;

  // assume data type
  IndexIterator<GenericKey<8>, RID, GenericComparator<8>> itr_;
  IndexIterator<GenericKey<8>, RID, GenericComparator<8>> itr_end_;
  IndexIterator<GenericKey<16>, RID, GenericComparator<16>> itr_16;
  IndexIterator<GenericKey<16>, RID, GenericComparator<16>> itr_end_16;
  // BPLUSTREE_INDEX_TYPE itr_;
  // BPLUSTREE_INDEX_TYPE itr_end_;
  std::string tbl_name_;
};
}  // namespace bustub
