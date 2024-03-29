//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_, BufferPoolManager *bpm, int index);
  ~IndexIterator();

  bool isEnd();

  const MappingType &operator*();

  IndexIterator &operator++();

  IndexIterator &operator=(const IndexIterator &other) {
    leaf_ = other.leaf_;
    buffer_pool_manager_ = other.buffer_pool_manager_;
    index_ = other.index_;

    if (leaf_ != nullptr) {
      buffer_pool_manager_->FetchPage(leaf_->GetPageId())->RLatch();
    }
    return *this;
  }

  bool operator==(const IndexIterator &itr) const {
    if (itr.leaf_ == nullptr && leaf_ == nullptr) {
      return true;
    }
    if (itr.leaf_ == nullptr) {
      return false;
    }
    if (leaf_ == nullptr) {
      return false;
    }
    return ((leaf_->GetPageId() == (itr.leaf_)->GetPageId()) && (index_ == itr.index_));
  }

  bool operator!=(const IndexIterator &itr) const {
    if (itr.leaf_ == nullptr && leaf_ == nullptr) {
      return false;
    }
    if (itr.leaf_ == nullptr) {
      return true;
    }
    if (leaf_ == nullptr) {
      return true;
    }
    return ((leaf_->GetPageId() != (itr.leaf_)->GetPageId()) || (index_ != itr.index_));
  }

 private:
  // add your own private member variables here
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_{nullptr};
  BufferPoolManager *buffer_pool_manager_{nullptr};
  int index_{-1};
};

}  // namespace bustub
