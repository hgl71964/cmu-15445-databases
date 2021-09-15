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

#include "common/logger.h"
#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan->TableOid())),
      child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  LOG_INFO("Update table: %s", table_info_->name_.c_str());
  child_executor_->Init();
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  while (child_executor_->Next(tuple, rid)) {
    Tuple tmp_tuple = *tuple;
    RID tmp_rid = *rid;

    // get updated tuple
    Tuple updated_tuple = GenerateUpdatedTuple(tmp_tuple);
    *tuple = updated_tuple;

    // update tuple
    bool ok = table_info_->table_->UpdateTuple(updated_tuple, tmp_rid, GetExecutorContext()->GetTransaction());

    // update index if necessary
    if (ok) {
      for (auto &index_info : GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info_->name_)) {
        auto *tree_index =
            dynamic_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(index_info->index_.get());

        // table tuple -> index key
        auto index_K_tmp =
            tmp_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, tree_index->GetKeyAttrs());
        auto index_K =
            updated_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, tree_index->GetKeyAttrs());

        tree_index->DeleteEntry(index_K_tmp, tmp_rid, GetExecutorContext()->GetTransaction());
        tree_index->InsertEntry(index_K, tmp_rid, GetExecutorContext()->GetTransaction());
      }
    }
  }
  return false;
}

}  // namespace bustub
