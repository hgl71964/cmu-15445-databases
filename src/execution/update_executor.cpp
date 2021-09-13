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
  if (child_executor_->Next(tuple, rid)) {
    Tuple tmp_tuple = *tuple;
    RID tmp_rid = *rid;

    // LOG_INFO("before %s %d", tmp_tuple.GetRid().ToString().c_str(), tmp_tuple.GetLength());

    // get updated tuple
    Tuple updated_tuple = GenerateUpdatedTuple(tmp_tuple);
    *tuple = updated_tuple;

    // update tuple
    bool ok = table_info_->table_->UpdateTuple(updated_tuple, tmp_rid, GetExecutorContext()->GetTransaction());
    // bool ok = table_info_->table_->MarkDelete(tmp_rid, GetExecutorContext()->GetTransaction());
    // RID ridd;
    // table_info_->table_->InsertTuple(updated_tuple, &ridd, GetExecutorContext()->GetTransaction());

    // update index if necessary
    if (ok) {
      for (auto &index_info : GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info_->name_)) {
        auto *tree_index =
            dynamic_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(index_info->index_.get());

        auto index_K_tmp =
            tmp_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, tree_index->GetKeyAttrs());
        auto index_K =
            updated_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, tree_index->GetKeyAttrs());

        tree_index->DeleteEntry(index_K_tmp, tmp_rid, GetExecutorContext()->GetTransaction());
        tree_index->v_InsertEntry(index_K, tmp_rid, GetExecutorContext()->GetTransaction());

        // {
        //   std::vector<RID> result;
        //   result.clear();
        //   tree_index->v_ScanKey(updated_tuple, &result, GetExecutorContext()->GetTransaction());
        // }
      }
    }
    return ok;
  }
  return false;
}

}  // namespace bustub
