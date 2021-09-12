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

    //{
    //  LOG_INFO("%d %d", tmp_tuple.GetValue(GetOutputSchema(), GetOutputSchema()->GetColIdx("colA")).GetAs<int32_t>(),
    //           tmp_tuple.GetValue(GetOutputSchema(), GetOutputSchema()->GetColIdx("colB")).GetAs<int32_t>());
    //  LOG_INFO("%d %d",
    //           updated_tuple.GetValue(GetOutputSchema(), GetOutputSchema()->GetColIdx("colA")).GetAs<int32_t>(),
    //           updated_tuple.GetValue(GetOutputSchema(), GetOutputSchema()->GetColIdx("colB")).GetAs<int32_t>());
    //}

    // LOG_INFO("generated %s %d %d", updated_tuple.GetRid().ToString().c_str(), updated_tuple.GetLength(),
    //          updated_tuple.GetRid().GetPageId());
    // LOG_INFO("%s", tmp_tuple.GetRid().ToString().c_str());

    // update tuple
    bool ok = table_info_->table_->UpdateTuple(updated_tuple, tmp_rid, GetExecutorContext()->GetTransaction());

    // LOG_INFO("updated %s %d", updated_tuple.GetRid().ToString().c_str(), updated_tuple.GetLength());
    // LOG_INFO("rid %s %d", tmp_rid.ToString().c_str(), ok);

    // update index if necessary
    if (ok) {
      *tuple = updated_tuple;
      for (auto &index_info : GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info_->name_)) {
        auto *tree_index =
            dynamic_cast<BPlusTreeIndex<GenericKey<32>, RID, GenericComparator<32>> *>(index_info->index_.get());

        auto index_K_tmp =
            tmp_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, tree_index->GetKeyAttrs());
        auto index_K =
            updated_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, tree_index->GetKeyAttrs());

        tree_index->DeleteEntry(index_K_tmp, tmp_rid, GetExecutorContext()->GetTransaction());
        tree_index->DeleteEntry(index_K, tmp_rid, GetExecutorContext()->GetTransaction());
        tree_index->v_InsertEntry(index_K, tmp_rid, GetExecutorContext()->GetTransaction());

        // {
        //   std::vector<RID> result;
        //   result.clear();
        //   tree_index->v_ScanKey(updated_tuple, &result, GetExecutorContext()->GetTransaction());
        // }
      }
    }
    return true;
  }
  return false;
}

}  // namespace bustub
