//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/b_plus_tree_index.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <map>
#include <string>
#include <vector>

#include "storage/index/b_plus_tree.h"
#include "storage/index/index.h"

namespace bustub {

#define BPLUSTREE_INDEX_TYPE BPlusTreeIndex<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeIndex : public Index {
 public:
  BPlusTreeIndex(IndexMetadata *metadata, BufferPoolManager *buffer_pool_manager);

  void InsertEntry(const Tuple &key, RID rid, Transaction *transaction) override;
  void v_InsertEntry(const Tuple &key, RID rid, Transaction *transaction);

  void DeleteEntry(const Tuple &key, RID rid, Transaction *transaction) override;
  void v_DeleteEntry(const Tuple &key, RID rid, Transaction *transaction);

  void ScanKey(const Tuple &key, std::vector<RID> *result, Transaction *transaction) override;
  void v_ScanKey(const Tuple &key, std::vector<RID> *result, Transaction *transaction);

  INDEXITERATOR_TYPE GetBeginIterator();

  INDEXITERATOR_TYPE GetBeginIterator(const KeyType &key);

  INDEXITERATOR_TYPE GetEndIterator();

 protected:
  // comparator for key
  KeyComparator comparator_;
  // container
  BPlusTree<KeyType, ValueType, KeyComparator> container_;
};

}  // namespace bustub
