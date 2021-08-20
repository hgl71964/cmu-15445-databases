//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/rid.h"
#include "common/logger.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {

const bool b_debug_msg = true;


INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
        if (b_debug_msg) {
          LOG_DEBUG("internal max cap: %d - leaf max cap: %d", internal_max_size_, leaf_max_size_);
        }
      }

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_==INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, 
                              std::vector<ValueType> *result, 
                              Transaction *transaction) {

  if (b_debug_msg) {
    Page *page;
    BPlusTreePage *page_node;

    page = buffer_pool_manager_->FetchPage(root_page_id_);
    page_node = reinterpret_cast<BPlusTreePage *> (page->GetData());
    ToString(page_node, buffer_pool_manager_);
  }

  Page* page = FindLeafPage(key, false);

  if (page == nullptr) {
    return false;
  }

  auto *leaf_page_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *> (page->GetData());

  ValueType val;
  bool ok = leaf_page_node->Lookup(key, &val, comparator_);

  if (b_debug_msg) {
    LOG_DEBUG("key: %ld - find: %d - page_id: %d", key.ToString(), ok, leaf_page_node->GetPageId());
  }

  // result.resize(1);
  if (ok) {
    result->push_back(std::move(val));
  }

  // done using; not dirty
  buffer_pool_manager_->UnpinPage(leaf_page_node->GetPageId(), false);
  return ok;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {

  // 1. if empty start new tree
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  }

  // 2. insert - ok = no duplicate
  bool ok = InsertIntoLeaf(key, value, transaction);

  if (b_debug_msg){
    LOG_DEBUG("key: %ld - ok: %d", key.ToString(), ok);
    Page *page;
    BPlusTreePage *page_node;

    page = buffer_pool_manager_->FetchPage(root_page_id_);
    page_node = reinterpret_cast<BPlusTreePage *> (page->GetData());
    ToString(page_node, buffer_pool_manager_);
  }

  return ok;
}

/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {

  // ask for new root page
  page_id_t page_id;
  auto *root_page = new_root(&page_id, true);

  // init new tree (as leaf)
  auto *root_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *> (root_page->GetData());
  root_node->Init(page_id, INVALID_PAGE_ID, leaf_max_size_);

  // insert; do not need to handle duplicate
  root_node->Insert(key, value, comparator_);

  // done using; mark dirty
  buffer_pool_manager_->UnpinPage(root_node->GetPageId(), true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, 
                                    const ValueType &value,   
                                    Transaction *transaction) {
  
  auto *page = FindLeafPage(key, false);
  if (page == nullptr) {
    throw Exception(ExceptionType::INVALID, "b+ tree - InsertIntoLeaf");
  }

  // check if duplicate
  auto *leaf_page_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *> (page->GetData());
  ValueType val;
  bool exist = leaf_page_node->Lookup(key, &val, comparator_);
  if (exist) {
    // try to insert deplicate key
    buffer_pool_manager_->UnpinPage(leaf_page_node->GetPageId(), false);
    return false;
  }

  // insert
  leaf_page_node->Insert(key, val, comparator_);

  // if full, split leaf node
  if (leaf_page_node->GetMaxSize() == leaf_page_node->GetSize()) {

    B_PLUS_TREE_LEAF_PAGE_TYPE* new_leaf_page_node = split_leaf(leaf_page_node);

    auto partition_key = leaf_page_node->KeyAt(leaf_page_node->GetSize()-1); // partition key

    // recursively handle parent
    InsertIntoParent(leaf_page_node, partition_key, 
                      new_leaf_page_node, transaction);

    // done using; mark dirty
    buffer_pool_manager_->UnpinPage(new_leaf_page_node->GetPageId(), true);
  }
  
  // done using; mark dirty
  buffer_pool_manager_->UnpinPage(leaf_page_node->GetPageId(), true);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
B_PLUS_TREE_LEAF_PAGE_TYPE *BPLUSTREE_TYPE::split_leaf(B_PLUS_TREE_LEAF_PAGE_TYPE *node) {

  // new page
  page_id_t page_id;
  auto *page = buffer_pool_manager_->NewPage(&page_id);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Split out of mem");
  }

  // init new nodes
  auto *new_page_node = 
      reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *> (page->GetData());
  new_page_node->Init(page_id, node->GetParentPageId(), node->GetMaxSize());

  // move key & val pairs
  node->MoveHalfTo(new_page_node);
  node->SetNextPageId(new_page_node->GetPageId());

  // new page wull be used by caller, dont Unpin
  return new_page_node;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {

  // new page
  page_id_t page_id;
  auto *page = buffer_pool_manager_->NewPage(&page_id);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Split out of mem");
  }

  // init new nodes
  N *new_page_node = reinterpret_cast<N *> (page->GetData());
  new_page_node->Init(page_id, node->GetParentPageId(), node->GetMaxSize());

  // move key & val pairs
  node->MoveHalfTo(new_page_node, buffer_pool_manager_);

  // new page wull be used by caller, dont Unpin
  return new_page_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, 
                                      const KeyType &key, 
                                      BPlusTreePage *new_node,
                                      Transaction *transaction) {
  // get parent node id
  auto parent_id = old_node->GetParentPageId();

  // root - terminate recursion
  if (parent_id == INVALID_PAGE_ID) {

    page_id_t page_id;
    auto *root_page = new_root(&page_id, false);

    // init new root (as internal)
    auto *root_node = 
      reinterpret_cast<BPlusTreeInternalPage<KeyType, 
        page_id_t, KeyComparator> *> (root_page->GetData());
    root_node->Init(page_id, INVALID_PAGE_ID, internal_max_size_);

    // adopt
    root_node->PopulateNewRoot(old_node->GetPageId(), key, 
                                    new_node->GetPageId());
    old_node->SetParentPageId(page_id);
    new_node->SetParentPageId(page_id);

    // done using; dirty
    buffer_pool_manager_->UnpinPage(root_node->GetPageId(), true);
    return;
  }

  // else fetch parent page
  auto *page = buffer_pool_manager_->FetchPage(parent_id);
  auto *parent_page_node = 
    reinterpret_cast<BPlusTreeInternalPage<KeyType, 
      page_id_t, KeyComparator> *> (page->GetData());

  // insert into parent, adopt
  parent_page_node->InsertNodeAfter(old_node->GetPageId(), key, 
                                      new_node->GetPageId());
  new_node->SetParentPageId(parent_page_node->GetPageId()); 

  // after insert into parent, recursion check if full
  if (parent_page_node->GetMaxSize() < parent_page_node->GetSize()) {
    //LOG_DEBUG("error - parent_page_node->GetMaxSize(): %d, parent_page_node->GetSize(): %d", 
    //      parent_page_node->GetMaxSize(), parent_page_node->GetSize());

    auto *new_parent_page_node = Split(parent_page_node);
    auto partition_key = new_parent_page_node->KeyAt(0); // partition key
    //auto partition_key = parent_page_node->KeyAt(parent_page_node->GetSize()-1); // partition key
    InsertIntoParent(parent_page_node, partition_key, 
                      new_parent_page_node, transaction);
    buffer_pool_manager_->UnpinPage(new_parent_page_node->GetPageId(), true);
  }

  // done using; mark dirty
  buffer_pool_manager_->UnpinPage(parent_page_node->GetPageId(), true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  return false;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) { return false; }

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() { return INDEXITERATOR_TYPE(); }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::new_root(page_id_t *page_id, const bool new_tree) {

  auto *page = buffer_pool_manager_->NewPage(page_id);

  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "start new tree out of mem");
  }

  // mark this as root page id
  root_page_id_ = *page_id;

  // insert header page (meta data) 
  UpdateRootPageId(new_tree); // true for start a new tree

  return page;
}
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  //throw Exception(ExceptionType::NOT_IMPLEMENTED, "Implement this for test");
  if (IsEmpty()) {
    return nullptr;
  }

  Page *page;
  BPlusTreePage *page_node;
  page_id_t val;

  // root
  page = buffer_pool_manager_->FetchPage(root_page_id_);
  page_node = reinterpret_cast<BPlusTreePage *> (page->GetData());

  // if  root and leaf - new tree - return directly
  // if root ok, then recursively search
  while (!page_node->IsLeafPage()) {

    // data key must exist in internal node
    auto *internal_page_node = 
      reinterpret_cast<BPlusTreeInternalPage<KeyType, 
        page_id_t, KeyComparator> *> (page_node);

    val = (leftMost)? internal_page_node->ValueAt(0)
                    :internal_page_node->Lookup(key, comparator_);

    // if val is INVALID XXX
    //if (val == INVALID_PAGE_ID) {
    //  buffer_pool_manager_->UnpinPage(page_node->GetPageId(), false);
    //  return nullptr;
    //}

    // unpin current page and find next
    buffer_pool_manager_->UnpinPage(page_node->GetPageId(), false);
    page = buffer_pool_manager_->FetchPage(val);
    page_node = reinterpret_cast<BPlusTreePage *> (page->GetData());
  }
  return page;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
