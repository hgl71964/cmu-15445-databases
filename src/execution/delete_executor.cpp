//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "common/logger.h"
#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan->TableOid())) {}

void DeleteExecutor::Init() {
  LOG_INFO("DeleteExecutor %s", table_info_->name_.c_str());
  child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (child_executor_->Next(tuple, rid)) {
    Tuple tmp_tuple = *tuple;
    RID tmp_rid = *rid;

    // del tuple
    bool ok = table_info_->table_->MarkDelete(tmp_rid, GetExecutorContext()->GetTransaction());

    // update into index if necessary
    if (ok) {
      for (auto &index_info : GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info_->name_)) {
        auto *tree_index =
            dynamic_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(index_info->index_.get());
        auto index_K_tmp =
            tmp_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, tree_index->GetKeyAttrs());

        LOG_INFO("index_key %d key %d ", index_K_tmp.GetLength(), tmp_tuple.GetLength());
        tree_index->DeleteEntry(index_K_tmp, tmp_rid, GetExecutorContext()->GetTransaction());
      }
    }
    return true;
  }
  return false;
}

}  // namespace bustub
