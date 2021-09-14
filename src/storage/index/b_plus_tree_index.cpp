//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree_index.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/index/b_plus_tree_index.h"
#include "common/logger.h"

namespace bustub {
/*
 * Constructor
 */
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_INDEX_TYPE::BPlusTreeIndex(IndexMetadata *metadata, BufferPoolManager *buffer_pool_manager)
    : Index(metadata),
      comparator_(metadata->GetKeySchema()),
      container_(metadata->GetName(), buffer_pool_manager, comparator_) {}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_INDEX_TYPE::InsertEntry(const Tuple &key, RID rid, Transaction *transaction) {
  // construct insert index key
  KeyType index_key;
  if (sizeof(index_key.data_) < key.GetLength()) {
    LOG_ERROR("index_key %ld key %d ", sizeof(index_key.data_), key.GetLength());
  }
  index_key.SetFromKey(key);
  // LOG_INFO("InsertEntry %ld %d %d", index_key.ToString(), rid.GetPageId(), rid.GetSlotNum());

  container_.Insert(index_key, rid, transaction);
  // LOG_INFO("InsertEntry ok");
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_INDEX_TYPE::v_InsertEntry(const Tuple &key, RID rid, Transaction *transaction) {
  // construct insert index key
  KeyType index_key;
  if (sizeof(index_key.data_) < key.GetLength()) {
    LOG_ERROR("index_key %ld key %d ", sizeof(index_key.data_), key.GetLength());
  }
  index_key.SetFromKey(key);
  auto ok = container_.Insert(index_key, rid, transaction);
  LOG_INFO("insert %d", ok);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_INDEX_TYPE::DeleteEntry(const Tuple &key, RID rid, Transaction *transaction) {
  // construct delete index key
  // LOG_INFO("DeleteEntry");
  KeyType index_key;
  if (sizeof(index_key.data_) < key.GetLength()) {
    LOG_ERROR("index_key %ld key %d ", sizeof(index_key.data_), key.GetLength());
  }
  index_key.SetFromKey(key);

  container_.Remove(index_key, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_INDEX_TYPE::v_DeleteEntry(const Tuple &key, RID rid, Transaction *transaction) {
  LOG_INFO("del %s %d", key.GetRid().ToString().c_str(), key.GetLength());
  DeleteEntry(key, rid, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_INDEX_TYPE::ScanKey(const Tuple &key, std::vector<RID> *result, Transaction *transaction) {
  // construct scan index key
  // LOG_INFO("ScanKey");
  KeyType index_key;
  if (sizeof(index_key.data_) < key.GetLength()) {
    LOG_ERROR("index_key %ld key %d ", sizeof(index_key.data_), key.GetLength());
  }
  index_key.SetFromKey(key);

  container_.GetValue(index_key, result, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_INDEX_TYPE::v_ScanKey(const Tuple &key, std::vector<RID> *result, Transaction *transaction) {
  KeyType index_key;
  if (sizeof(index_key.data_) < key.GetLength()) {
    LOG_ERROR("index_key %ld key %d ", sizeof(index_key.data_), key.GetLength());
  }
  index_key.SetFromKey(key);

  auto ok = container_.GetValue(index_key, result, transaction);
  LOG_INFO("%s %d", result->at(0).ToString().c_str(), ok);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_INDEX_TYPE::GetBeginIterator() { return container_.begin(); }

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_INDEX_TYPE::GetBeginIterator(const KeyType &key) { return container_.Begin(key); }

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_INDEX_TYPE::GetEndIterator() { return container_.end(); }

template class BPlusTreeIndex<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeIndex<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeIndex<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeIndex<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
