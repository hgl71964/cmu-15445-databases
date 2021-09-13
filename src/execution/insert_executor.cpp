//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "common/logger.h"
#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      tbl_meta_(exec_ctx->GetCatalog()->GetTable(plan->TableOid())),
      inserted_(false) {}

void InsertExecutor::Init() {
  if (!plan_->IsRawInsert()) {
    child_executor_->Init();
  }
}

bool InsertExecutor::direct_insert() {
  if (!inserted_) {
    for (size_t i = 0; i < plan_->RawValues().size(); i++) {
      // instantiate tuple via Value vector
      auto val_vec = plan_->RawValuesAt(static_cast<uint32_t>(i));
      Tuple tmp_tuple(val_vec, &tbl_meta_->schema_);
      RID tmp_rid;

      // insert into table - populate tmp_rid
      tbl_meta_->table_->InsertTuple(tmp_tuple, &tmp_rid, GetExecutorContext()->GetTransaction());

      // insert into index if necessary
      for (auto &index_info : GetExecutorContext()->GetCatalog()->GetTableIndexes(tbl_meta_->name_)) {
        auto index_K_tmp =
            tmp_tuple.KeyFromTuple(tbl_meta_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        index_info->index_->InsertEntry(index_K_tmp, tmp_rid, GetExecutorContext()->GetTransaction());
      }
    }
    // mark inserted
    inserted_ = true;
    return true;
  }
  return false;
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  // one call and insert all??
  if (plan_->IsRawInsert()) {
    return direct_insert();
  }

  // instantiate tuple via child exec
  if (child_executor_->Next(tuple, rid)) {
    Tuple tmp_tuple = *tuple;
    RID tmp_rid;

    // insert into table - populate tmp_rid
    tbl_meta_->table_->InsertTuple(tmp_tuple, &tmp_rid, GetExecutorContext()->GetTransaction());

    // insert into index if necessary
    for (auto &index_info : GetExecutorContext()->GetCatalog()->GetTableIndexes(tbl_meta_->name_)) {
      auto index_K_tmp =
          tmp_tuple.KeyFromTuple(tbl_meta_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(index_K_tmp, tmp_rid, GetExecutorContext()->GetTransaction());
    }
    return true;
  }
  return false;
}

}  // namespace bustub
