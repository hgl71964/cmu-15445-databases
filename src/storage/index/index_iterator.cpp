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
INDEXITERATOR_TYPE::IndexIterator(page_id_t pid, 
                                BufferPoolManager *bpm,
                                int index)
:page_id(pid), buffer_pool_manager_(bpm), index(index), item() {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() { return page_id == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() { 

    if (isEnd()) {
        throw Exception(ExceptionType::INVALID, "iterator *");
    }

    auto *page = buffer_pool_manager_->FetchPage(page_id);
    auto* leaf_page= reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *> (page->GetData());

    MappingType m = leaf_page->GetItem(index);
    item = m;

    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return item;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
    if (isEnd()) {
        throw Exception(ExceptionType::INVALID, "iterator *");
    }

    auto *page = buffer_pool_manager_->FetchPage(page_id);
    auto *leaf_page= reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *> (page->GetData());

    if (index == (leaf_page->GetSize()-1)) {
        page_id = leaf_page->GetNextPageId();
        index = 0;
    } else {
        page_id = page_id;
        index++;
    }

    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
