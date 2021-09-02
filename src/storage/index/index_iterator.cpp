/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;
//: page_id(INVALID_PAGE_ID), buffer_pool_manager_(nullptr), index(0) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *leaf, BufferPoolManager *bpm, int index)
    : leaf_(leaf), buffer_pool_manager_(bpm), index_(index) {}

INDEX_TEMPLATE_ARGUMENTS
// INDEXITERATOR_TYPE::~IndexIterator() = default;
INDEXITERATOR_TYPE::~IndexIterator() {
  if (leaf_ != nullptr) {
    buffer_pool_manager_->FetchPage(leaf_->GetPageId())->RUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_->GetPageId(), false);
    buffer_pool_manager_->UnpinPage(leaf_->GetPageId(), false);
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() { return leaf_ == nullptr; }

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() {
  if (isEnd()) {
    throw Exception(ExceptionType::INVALID, "iterator *");
  }
  return leaf_->GetItem(index_);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  if (isEnd()) {
    throw Exception(ExceptionType::INVALID, "iterator *");
  }

  index_++;
  if (index_ >= leaf_->GetSize()) {
    auto next_pid = leaf_->GetNextPageId();
    buffer_pool_manager_->FetchPage(leaf_->GetPageId())->RUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_->GetPageId(), false);
    buffer_pool_manager_->UnpinPage(leaf_->GetPageId(), false);

    if (next_pid == INVALID_PAGE_ID) {
      leaf_ = nullptr;
    } else {
      auto *page = buffer_pool_manager_->FetchPage(next_pid);
      page->RLatch();
      leaf_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
      index_ = 0;
    }
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
