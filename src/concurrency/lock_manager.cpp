//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"
#include <utility>
#include <vector>
#include "common/logger.h"

namespace bustub {

// latch_ - global lock protect adding/deleting element in lock_table_

// XXX need global lock in each func?? - or tuple level lock for the map item is fine?
// XXX transaction check fail should result in an Exception??

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  // txn instantiate as growing state
  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  // 1. iso level no need S lock
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    return true;  // no S lock
  }

  // 2. others need S lock - make record - add to txn
  LockRequest lr(txn->GetTransactionId(), LockMode::SHARED);
  txn->GetSharedLockSet()->emplace(rid);

  // hold global lock
  latch_.lock();

  // 3. acquire tuple-level lock - this will implicitly 'instantiate' at rid
  std::unique_lock<std::mutex> tuple_level_lock(rid_lock_[rid]);

  // 4. append to queue
  lock_table_[rid].request_queue_.push_back(lr);
  auto *p = &lock_table_[rid].request_queue_.back();
  rid_set_.insert(rid);

  // release global lock
  latch_.unlock();

  // 5. wake up loop
  for (;;) {
    // see if ok to grantd - if rid does not exist - no iteration
    bool ok = true;
    for (auto &item : lock_table_[rid].request_queue_) {
      if (item.lock_mode_ == LockMode::EXCLUSIVE && item.granted_) {  // if any is X, we cannot share this
        ok = false;
        break;
      }
    }
    p->granted_ = ok;

    // sleep if not ok
    if (ok) {
      break;
    }
    lock_table_[rid].cv_.wait(tuple_level_lock);  // sleep and release - wake up and hold
  }
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  // txn instantiate as growing state
  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  // ALL need X lock ?? XXX
  // 2. all get X lock - make record - add to txn
  LockRequest lr(txn->GetTransactionId(), LockMode::EXCLUSIVE);
  txn->GetExclusiveLockSet()->emplace(rid);

  // hold global lock
  latch_.lock();

  // 3. create tuple-level lock - this will implicitly 'instantiate' at rid
  std::unique_lock<std::mutex> tuple_level_lock(rid_lock_[rid]);

  // 4. append to queue
  lock_table_[rid].request_queue_.push_back(lr);
  auto *p = &lock_table_[rid].request_queue_.back();
  rid_set_.insert(rid);

  // release global lock
  latch_.unlock();

  // 5. block until acquire
  for (;;) {
    // see if ok to grantd - if rid does not exist - no iteration
    bool ok = true;
    for (auto &item : lock_table_[rid].request_queue_) {
      if (item.granted_) {  // if any is holding, we cannot get this
        ok = false;
        break;
      }
    }
    p->granted_ = ok;

    // sleep if not ok
    if (ok) {
      break;
    }
    lock_table_[rid].cv_.wait(tuple_level_lock);  // sleep and release - wake up and hold
  }
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  // txn instantiate as growing state
  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  // hold global lock
  latch_.lock();

  // tuple-level lock
  std::unique_lock<std::mutex> tuple_level_lock(rid_lock_[rid]);

  // check if someone if upgrading, abort if true
  if (lock_table_[rid].upgrading_) {
    if (txn->GetState() != TransactionState::ABORTED) {
      LOG_INFO("abort upgrade %d", txn->GetTransactionId());
    }
    txn->SetState(TransactionState::ABORTED);
    latch_.unlock();
    return false;
  }
  lock_table_[rid].upgrading_ = true;

  // start upgrade
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  LockRequest lr(txn->GetTransactionId(), LockMode::EXCLUSIVE);

  // erase old request in queue - if granted unlock ?? FIXME
  queue_gcL(rid, txn->GetTransactionId());

  // 4. append to queue
  lock_table_[rid].request_queue_.push_back(lr);
  auto *p = &lock_table_[rid].request_queue_.back();

  // release global lock
  latch_.unlock();

  // 5. block until acquire
  for (;;) {
    // see if ok to grantd - if rid does not exist - no iteration
    bool ok = true;
    for (auto &item : lock_table_[rid].request_queue_) {
      if (item.granted_) {  // if any is holding, we cannot get this
        ok = false;
        break;
      }
    }
    p->granted_ = ok;

    // sleep if not ok
    if (ok) {
      break;
    }
    lock_table_[rid].cv_.wait(tuple_level_lock);  // sleep and release - wake up and hold
  }

  // if upgrade successful - mark no one is trying to upgrade
  lock_table_[rid].upgrading_ = false;
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);

  // check if set state to txn
  if (txn->GetState() == TransactionState::GROWING) {
    // print_txn_state(txn);
    // print_iso_level(txn);
    /** XXX not sure why need this condition - it seems 2-LP is done by app logic
     * NOTE: slide says S lock will released immediately by READ_COMMITTED txn
     */
    if (txn->GetIsolationLevel() != IsolationLevel::READ_COMMITTED) {
      txn->SetState(TransactionState::SHRINKING);
    }
  }
  // LOG_INFO("unlock %d", txn->GetTransactionId());

  latch_.lock();
  rid_lock_[rid].lock();
  queue_gcL(rid, txn->GetTransactionId());  // gc - del my request
  rid_lock_[rid].unlock();
  latch_.unlock();

  // notify - let others run
  lock_table_[rid].cv_.notify_all();

  // anything else? XXX

  return true;
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {}

bool LockManager::HasCycle(txn_id_t *txn_id) { return false; }

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() { return {}; }

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::unique_lock<std::mutex> lock_manager_global_lock(latch_);
      // (student): remove the continue and add your cycle detection and abort code here
      build_graphL();
      continue;
    }
  }
}

/*NOTE: global lock is held */
void LockManager::build_graphL() {
  lock_all_tuplesL();

  // now have global lock + all valid RID lock

  // build graph
  // for (auto &item: lock_table_) {
  //   for (auto &queue_elem : request_queue_) {
  //     if ()
  //   }
  // }

  unlock_all_tuplesL();
}

void LockManager::queue_gcL(const RID &rid, txn_id_t txn_id) {
  for (auto itr = lock_table_[rid].request_queue_.cbegin(); itr != lock_table_[rid].request_queue_.cend(); ++itr) {
    if (itr->txn_id_ == txn_id) {
      lock_table_[rid].request_queue_.erase(itr);
      break;
    }
  }

  // maintain valid RID set
  if (lock_table_[rid].request_queue_.empty()) {
    rid_set_.erase(rid);
  }
}

void LockManager::lock_all_tuplesL() {
  for (auto &item : rid_set_) {
    rid_lock_[item].lock();
  }
}

void LockManager::unlock_all_tuplesL() {
  for (auto &item : rid_set_) {
    rid_lock_[item].unlock();
  }
}

void LockManager::print_txn_state(Transaction *txn) {
  switch (txn->GetState()) {
    case TransactionState::GROWING:
      LOG_INFO("GROWING");
      break;
    case TransactionState::SHRINKING:
      LOG_INFO("SHRINKING");
      break;
    case TransactionState::COMMITTED:
      LOG_INFO("COMMITTED");
      break;
    case TransactionState::ABORTED:
      LOG_INFO("ABORTED");
      break;
    default:
      LOG_INFO("fatal print_txn_state");
      break;
  }
}
void LockManager::print_iso_level(Transaction *txn) {
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::READ_UNCOMMITTED:
      LOG_INFO("READ_UNCOMMITTED");
      break;
    case IsolationLevel::READ_COMMITTED:
      LOG_INFO("READ_COMMITTED");
      break;
    case IsolationLevel::REPEATABLE_READ:
      LOG_INFO("REPEATABLE_READ");
      break;
    default:
      LOG_INFO("fatal print_iso_level");
      break;
  }
}

}  // namespace bustub
