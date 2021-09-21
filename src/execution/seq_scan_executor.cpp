//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"
#include "common/logger.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(GetExecutorContext()->GetCatalog()->GetTable(plan_->GetTableOid())),
      done_(false) {}

void SeqScanExecutor::Init() {
  LOG_INFO("Seqscan %s - txn: %d", table_info_->name_.c_str(),
           GetExecutorContext()->GetTransaction()->GetTransactionId());
  auto itr = table_info_->table_->Begin(GetExecutorContext()->GetTransaction());
  if (itr == table_info_->table_->End()) {
    done_ = true;
  } else {
    rid_ = itr->GetRid();
  }

  LOG_INFO("%s", table_info_->schema_.ToString().c_str());
  LOG_INFO("%s", GetOutputSchema()->ToString().c_str());
}

/**
 * NOTE: it is the app logic to request lock and unlock
 * which depends on iso-level and executor type
 */
void SeqScanExecutor::lock(const RID &rid) {
  if ((GetExecutorContext()->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) &&
      (!GetExecutorContext()->GetTransaction()->IsSharedLocked(rid)) &&
      (!GetExecutorContext()->GetTransaction()->IsExclusiveLocked(rid))) {
    auto get_lock = GetExecutorContext()->GetLockManager()->LockShared(GetExecutorContext()->GetTransaction(), rid);
    if (!get_lock) {
      LOG_INFO("get_lock not ok - txn: %d", GetExecutorContext()->GetTransaction()->GetTransactionId());
    }
  }
}
void SeqScanExecutor::unlock(const RID &rid) {
  // if exclusively lock, it is not here to release
  if (GetExecutorContext()->GetTransaction()->IsSharedLocked(rid) &&
      GetExecutorContext()->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
    GetExecutorContext()->GetLockManager()->Unlock(GetExecutorContext()->GetTransaction(), rid);
  }
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  if (done_) {
    return false;
  }
  const Schema *schema = GetOutputSchema();

  auto itr = TableIterator(table_info_->table_.get(), rid_, GetExecutorContext()->GetTransaction());
  while (itr != table_info_->table_->End()) {
    // LOCK
    auto lock_rid = itr->GetRid();
    lock(lock_rid);

    // get tuple
    auto tmp_tuple = *itr;

    // format output tuple
    std::vector<Value> res;
    for (const Column &col : schema->GetColumns()) {
      Value val = tmp_tuple.GetValue(schema, schema->GetColIdx(col.GetName()));  // XXX not sure if general enough
      // Value val = tmp_tuple.GetValue(&table_info_->schema_, table_info_->schema_.GetColIdx(col.GetName()));
      res.push_back(val);
    }
    Tuple new_tuple(res, schema);

    // UNLOCK
    unlock(lock_rid);

    // incr
    ++itr;
    if (itr == table_info_->table_->End()) {
      done_ = true;
    } else {
      rid_ = itr->GetRid();
    }

    // eval predicate
    auto *p = plan_->GetPredicate();  // could be nullptr
    if (p == nullptr || plan_->GetPredicate()->Evaluate(&new_tuple, schema).GetAs<bool>()) {
      *tuple = new_tuple;
      *rid = tmp_tuple.GetRid();  // seems ok, outputSchema is changed but RID is the same
      return true;
    }
  }
  return false;
}

}  // namespace bustub
