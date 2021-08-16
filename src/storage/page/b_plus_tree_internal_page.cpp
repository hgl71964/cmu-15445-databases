//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/config.h"
#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "type/type_id.h"
#include "common/logger.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  BPlusTreePage::SetPageType(IndexPageType::INTERNAL_PAGE);
  BPlusTreePage::SetPageId(page_id);
  BPlusTreePage::SetParentPageId(parent_id);
  BPlusTreePage::SetMaxSize(max_size);

  // XXX init page set??
  BPlusTreePage::SetSize(0); 
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  if (index < 0 || index >= BPlusTreePage::GetSize()) {
    LOG_ERROR("internal Page KeyAt");
  }
  return array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  if (index < 0 || index >= BPlusTreePage::GetSize()) {
    LOG_ERROR("internal Page SetKeyAt");
  }
  array[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  for (int i = 0; i < BPlusTreePage::GetSize(); i++) {
    if (array[i].second == value) {
      return i;
    }
  }
  return -1; // XXX cannot find?
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
  if (index < 0 || index >= BPlusTreePage::GetSize()) {
    LOG_ERROR("internal Page ValueAt");
  }
  return array[index].second;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, 
                                                const KeyComparator &comparator) const {

  // this tree does not allow duplicates key
  // TODO binary search

  for (int i = 1; i < BPlusTreePage::GetSize(); i++) {
    if (comparator(array[i].first, key) == 0) {
      return array[i].second;
    }
  }
  return INVALID_PAGE_ID; // XXX cannot find
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
* Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, 
                                                     const KeyType &new_key,
                                                     const ValueType &new_value) {
  // root node should have 1 key 2 pointers - and the first key is invalid
  array[0].second = old_value;
  array[1].first = new_key;
  array[1].second = new_value;
  BPlusTreePage::SetSize(2);
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, 
                                                    const KeyType &new_key,
                                                    const ValueType &new_value) {
  
  if (BPlusTreePage::GetSize() >= BPlusTreePage::GetMaxSize()) {
    LOG_ERROR("internal InsertNodeAfter size");
  }

  auto index = ValueIndex(old_value);
  BPlusTreePage::IncreaseSize(1);

  for (int i = BPlusTreePage::GetSize()-1; i > index + 1; i--) {
   array[i].first = array[i-1].first;
   array[i].second = array[i-1].second;
  }
  array[index+1].first = new_key;
  array[index+1].second = new_value;
  return BPlusTreePage::GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {

  // during split, right half need to be moved to a new page
  //
  auto size = BPlusTreePage::GetSize();

  int move_size = size - size/2;
  int start_index = size/2;

  // move half to; assume recipient is a new page
  recipient.CopyNFrom(&array[start_index], move_size, buffer_pool_manager);

  // update self
  BPlusTreePage::SetSize(size/2);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
* Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
* So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
*/
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, 
                                              int size, 
                                              BufferPoolManager *buffer_pool_manager) {
  for (int i = 0; i < size; i++) {
      array[i] = *(items+i);  // copy

      // adopt
      auto page_id = array[i].second;
      auto *child_page = buffer_pool_manager.FetchPageImpl(page_id);
      BPlusTreePage *b_plus_child_page = reinterpret_cast<BPlusTreePage *> (child_page.GetData());
      b_plus_child_page->SetParentPageId(BPlusTreePage::GetPageId());
      buffer_pool_manager.UnpinPageImpl(b_plus_child_page->GetPageId(), true); // mark dirty
    }

  // update self (because copy all N)
  BPlusTreePage::SetSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  if (index < 0 || index >= BPlusTreePage::GetSize()) {
    LOG_ERROR("internal Page Remove");
  }

  auto size = BPlusTreePage::GetSize();
  for (int i = index; i < size - 1; i++) {
    array[i].first = array[i+1].first;
    array[i].second = array[i+1].second;
  }

  // update self
  BPlusTreePage::SetSize(size-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {

  // with assumption that there is the only ONE key val pair
  auto val = array[0].second;

  BPlusTreePage::SetSize(0);
  return val; 
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, 
                                               const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {

  auto size = BPlusTreePage::GetSize();

  // TODO

  // update self
  BPlusTreePage::SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, 
                                                      const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  auto size = BPlusTreePage::GetSize();
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  auto size = BPlusTreePage::GetSize();
  BPlusTreePage::IncreaseSize(1);
  array[size] = pair;

  // adopt
  auto page_id = array[size].second;
  auto *child_page = buffer_pool_manager.FetchPageImpl(page_id);
  BPlusTreePage *b_plus_child_page = reinterpret_cast<BPlusTreePage *> (child_page.GetData());
  b_plus_child_page->SetParentPageId(BPlusTreePage::GetPageId());
  buffer_pool_manager.UnpinPageImpl(b_plus_child_page->GetPageId(), true); // mark dirty
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, 
                                                       const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  auto size = BPlusTreePage::GetSize();
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  auto size = BPlusTreePage::GetSize();
  BPlusTreePage::IncreaseSize(1);

  for (int i = size; i > 0; i--) {
    array[i].first = array[i-1].first;
    array[i].second = array[i-1].second;
  }
  array[0] = pair;

  // adopt
  auto page_id = array[0].second;
  auto *child_page = buffer_pool_manager.FetchPageImpl(page_id);
  BPlusTreePage *b_plus_child_page = reinterpret_cast<BPlusTreePage *> (child_page.GetData());
  b_plus_child_page->SetParentPageId(BPlusTreePage::GetPageId());
  buffer_pool_manager.UnpinPageImpl(b_plus_child_page->GetPageId(), true); // mark dirty
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
