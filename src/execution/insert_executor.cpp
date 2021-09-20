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
      done_(false) {}

void InsertExecutor::Init() {
  LOG_INFO("%s", tbl_meta_->name_.c_str());
  if (!plan_->IsRawInsert()) {
    child_executor_->Init();
  }
  LOG_INFO("%s", tbl_meta_->schema_.ToString().c_str());
  if (GetOutputSchema() != nullptr && GetOutputSchema()->GetColumnCount() > 0) {
    LOG_INFO("%s", GetOutputSchema()->ToString().c_str());
  }
}

void InsertExecutor::direct_insert() {
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
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (done_) {
    return false;
  }
  done_ = true;

  // one call and insert all
  if (plan_->IsRawInsert()) {
    direct_insert();
    return false;
  }

  // instantiate tuple via child exec
  while (child_executor_->Next(tuple, rid)) {
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
  }
  return false;
}

}  // namespace bustub
