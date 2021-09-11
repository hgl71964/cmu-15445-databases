//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan->TableOid())),
      child_executor_(child_executor) {}

void UpdateExecutor::Init() { child_executor_->Init(); }

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (child_executor_->Next(tuple, rid)) {
    Tuple tmp_tuple = *tuple;
    RID tmp_rid;

    // insert into table - populate tmp_rid
    tbl_meta_->table_->InsertTuple(tmp_tuple, &tmp_rid, GetExecutorContext()->GetTransaction());

    // insert into index if necessary
    for (auto &index_info : GetExecutorContext()->GetCatalog()->GetTableIndexes(tbl_meta_->name_)) {
      auto *tree_index =
          dynamic_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(index_info->index_.get());
      tree_index->InsertEntry(tmp_tuple, tmp_rid, GetExecutorContext()->GetTransaction());
    }
    return true;
  }
  return false;
}

}  // namespace bustub
