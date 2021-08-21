//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  BPlusTreePage::SetPageType(IndexPageType::LEAF_PAGE);
  BPlusTreePage::SetPageId(page_id);
  BPlusTreePage::SetParentPageId(parent_id);
  BPlusTreePage::SetMaxSize(max_size);
  BPlusTreePage::SetSize(0); 

  next_page_id_ = INVALID_PAGE_ID; 
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
}

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
**/
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, 
                                         const KeyComparator &comparator) const {
  
  for (int i = 0; i < BPlusTreePage::GetSize(); i++) {
    if (comparator(array[i].first, key)!=-1) {
      return i;
    }
  }
  return -1; // XXX??
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  return array[index].first;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  // replace with your own code
  return array[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, 
                                       const KeyComparator &comparator) {
  int keyidx = 0;
  for (int i = 0; i < BPlusTreePage::GetSize(); i++) {
    if (comparator(array[i].first, key) == -1) {
      keyidx++;
    } else {
      break;
    }
  }
  BPlusTreePage::IncreaseSize(1);

  /**
  NOTE: unique key
  **/
  for (int i = BPlusTreePage::GetSize()-1; i>keyidx; i--) {
    array[i].first = array[i-1].first;
    array[i].second = array[i-1].second;
  }
  array[keyidx].first = key;
  array[keyidx].second = value;

  //LOG_DEBUG("key: %ld - val: %d - insert index: %d - page_id: %d", key.ToString(), value.GetSlotNum(), keyidx, GetPageId());

  return BPlusTreePage::GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient) {

  // during split, right half need to be moved to a new page
  //
  auto size = BPlusTreePage::GetSize();

  int move_size = size - size/2;
  int start_index = size/2;

  //LOG_DEBUG("key: %ld, val: %d", array[0].first.ToString(), array[0].second.GetSlotNum());
  //LOG_DEBUG("key: %ld, val: %d", array[start_index].first.ToString(), array[start_index].second.GetSlotNum());

  // move half to; assume recipient is a new page
  recipient->CopyNFrom(&array[start_index], move_size);

  // update self
  BPlusTreePage::SetSize(size/2);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {

  for (int i = 0; i < size; i++) {
      array[i] = *(items+i);  // copy
    }
  BPlusTreePage::SetSize(size); // update self (because copy all N)
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType *value, 
                                        const KeyComparator &comparator) const {
  
  // TODO binary search
  for (int i = 0; i < BPlusTreePage::GetSize(); i++) {
    if (comparator(key, array[i].first) == 0) {
      *value = array[i].second;
      return true;
    }
  }
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, 
                                                      const KeyComparator &comparator) {

  /**
  NOTE: assume the keys are unique
  **/

  int id = -1;
  auto size = BPlusTreePage::GetSize();
  for (int i = 0; i < size; i++) {
    if (comparator(key, array[i].first) == 0) {
      id = i;
      break;
    }
  }

  if (id != -1) {
    for (int i = id; i < size-1; i++) {
      array[i].first = array[i+1].first;
      array[i].second = array[i+1].second;
    }

    // update self
    BPlusTreePage::IncreaseSize(-1);
  }
  return BPlusTreePage::GetSize(); 
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient) {
  // TODO
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient) {

  recipient->CopyLastFrom(array[0]);

  for (int i = 0; i < BPlusTreePage::GetSize()-1; i++) {
    array[i].first = array[i+1].first;
    array[i].second = array[i+1].second;
  }
  BPlusTreePage::IncreaseSize(-1);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  array[BPlusTreePage::GetSize()] = item;
  BPlusTreePage::IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient) {
  recipient->CopyFirstFrom(array[BPlusTreePage::GetSize()-1]);
  BPlusTreePage::IncreaseSize(-1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {

  auto size = BPlusTreePage::GetSize();

  // this assume the insert key < original key[0]
  for (int i = size; i > 0; i--) {
    array[i].first = array[i-1].first;
    array[i].second = array[i-1].second;
  }
  array[0] = item;
  BPlusTreePage::IncreaseSize(1);
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
