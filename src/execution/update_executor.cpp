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
      child_executor_(std::move(child_executor)),
      done(false) {}

void UpdateExecutor::Init() { child_executor_->Init(); }

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (done) {
    return false;
  }
  while (child_executor_->Next(tuple, rid)) {
    Tuple tmp_tuple = *tuple;
    RID tmp_rid = *rid;

    // LOG_INFO("before %s %d", tmp_tuple.GetRid().ToString().c_str(), tmp_tuple.GetLength());
    {
      RID t;
      LOG_INFO("%d", t.GetPageId());
    }

    // get updated tuple
    Tuple updated_tuple = GenerateUpdatedTuple(tmp_tuple);

    // LOG_INFO("generated %s %d %d", updated_tuple.GetRid().ToString().c_str(), updated_tuple.GetLength(),
    //          updated_tuple.GetRid().GetPageId());
    // LOG_INFO("%s", tmp_tuple.GetRid().ToString().c_str());

    // update tuple
    bool ok = table_info_->table_->UpdateTuple(updated_tuple, tmp_rid, GetExecutorContext()->GetTransaction());

    // LOG_INFO("updated %s %d", updated_tuple.GetRid().ToString().c_str(), updated_tuple.GetLength());
    // LOG_INFO("rid %s %d", tmp_rid.ToString().c_str(), ok);

    // {
    //   Tuple t;
    //   table_info_->table_->GetTuple(tmp_rid, &t, GetExecutorContext()->GetTransaction());
    //   LOG_INFO("get %s %d", t.GetRid().ToString().c_str(), t.GetLength());
    // }

    // update index if necessary
    if (ok) {
      *tuple = updated_tuple;
      for (auto &index_info : GetExecutorContext()->GetCatalog()->GetTableIndexes(table_info_->name_)) {
        auto *tree_index =
            dynamic_cast<BPlusTreeIndex<GenericKey<32>, RID, GenericComparator<32>> *>(index_info->index_.get());
        tree_index->DeleteEntry(tmp_tuple, tmp_rid, GetExecutorContext()->GetTransaction());
        tree_index->DeleteEntry(updated_tuple, tmp_rid, GetExecutorContext()->GetTransaction());
        tree_index->v_InsertEntry(updated_tuple, tmp_rid, GetExecutorContext()->GetTransaction());

        // {
        //   std::vector<RID> result;
        //   result.clear();
        //   tree_index->v_ScanKey(updated_tuple, &result, GetExecutorContext()->GetTransaction());
        // }
      }
    }
  }
  done = true;
  return true;
}

}  // namespace bustub
