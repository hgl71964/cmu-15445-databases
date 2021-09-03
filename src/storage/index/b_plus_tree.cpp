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
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {

namespace {
const bool b_debug_msg = false;
}

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      virtual_root_id_(INVALID_PAGE_ID) {
  // clean bpm
  buffer_pool_manager_->init_gc();
  // virtual root init
  auto *page = new_page(&virtual_root_id_);
  auto *root_node = reinterpret_cast<InternalPage *>(page->GetData());
  root_node->Init(virtual_root_id_, INVALID_PAGE_ID, 0);
  buffer_pool_manager_->UnpinPage(virtual_root_id_, true);

  if (b_debug_msg) {
    LOG_DEBUG("internal max cap: %d - leaf max cap: %d", internal_max_size_, leaf_max_size_);
    LOG_DEBUG("virtual root id: %d", virtual_root_id_);
  }
}

/*
 * Helper function to decide whether current b+tree is empty - CALLER HOLD lock
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  //{
  //  Page *page;
  //  BPlusTreePage *page_node;

  //  page = buffer_pool_manager_->FetchPage(root_page_id_);
  //  page_node = reinterpret_cast<BPlusTreePage *> (page->GetData());
  //  ToString(page_node, buffer_pool_manager_);
  //}
  mu_.lock();

  Page *page = READ_FindLeafPage(key, false, transaction);

  if (page == nullptr) {
    mu_.unlock();
    return false;
  }

  auto *leaf_page_node = reinterpret_cast<LeafPage *>(page->GetData());

  ValueType val;
  bool ok = leaf_page_node->Lookup(key, &val, comparator_);

  if (ok) {
    result->push_back(std::move(val));
  }

  // done using; not dirty
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(leaf_page_node->GetPageId(), false);
  mu_.unlock();
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
  // lock();
  // if (IsEmpty()) {
  //  StartNewTree(key, value);  // hold virtual root thoughtout
  //  unlock();
  //  return true;
  //}
  // unlock();
  mu_.lock();
  check_txns(transaction);

  // 2. insert - ok = no duplicate
  bool ok = InsertIntoLeaf(key, value, transaction);

  mu_.unlock();
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
  auto *root_page = new_rootL(true);

  // init new tree (as leaf)
  auto *root_node = reinterpret_cast<LeafPage *>(root_page->GetData());
  root_node->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);

  if (root_page->GetPageId() == virtual_root_id_) {
    LOG_ERROR("new tree? %d %d", root_page->GetPageId(), virtual_root_id_);
  }
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
/*NOTE: for insert, if a node is modified, its ancestor also got modified if the ancestor in transaction*/
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  // fetch - page hold WRITE latch
  auto *page = WRITE_FindLeafPage_new(key, value, false, WType::INSERT, transaction);
  if (page == nullptr) {
    return true;
  }

  // check if duplicate
  auto *leaf_page_node = reinterpret_cast<LeafPage *>(page->GetData());
  ValueType val;
  bool exist = leaf_page_node->Lookup(key, &val, comparator_);
  if (exist) {
    release_N_unPin(page, transaction, false);  // page, ancestor not dirty
    return false;
  }

  // insert
  auto new_size = leaf_page_node->Insert(key, value, comparator_);

  // if full, split leaf node - now parent latch must been held
  if (new_size >= leaf_page_node->GetMaxSize()) {
    check_parent(leaf_page_node, transaction);
    LeafPage *new_leaf_page_node = Split(leaf_page_node, transaction);

    auto partition_key = new_leaf_page_node->KeyAt(0);  // partition key

    // recursively insert parent
    InsertIntoParent(leaf_page_node, partition_key, new_leaf_page_node, transaction);

    // mark dirty
    // buffer_pool_manager_->UnpinPage(new_leaf_page_node->GetPageId(), true);
  }

  // done using; mark dirty;
  release_N_unPin(page, transaction, true);  // page, ancestor dirty
  return true;
}

/*
// no need to hold latch for newly split page here
// because it is the right sibling,
*/
/*WHen this is called, its left and parent have latch, so it is safe not to hold latch*/
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node, Transaction *transaction) {
  // new page
  page_id_t page_id;
  auto *page = new_page(&page_id);
  page->WLatch();
  transaction->AddIntoPageSet(page);

  // init new nodes
  N *new_page_node = reinterpret_cast<N *>(page->GetData());
  new_page_node->Init(page_id, node->GetParentPageId(), node->GetMaxSize());

  if (node->IsLeafPage()) {
    auto *tmp_n = reinterpret_cast<LeafPage *>(new_page_node);
    auto *tmp = reinterpret_cast<LeafPage *>(node);

    // move key & val pairs
    tmp->MoveHalfTo(tmp_n);
    auto pid = tmp->GetNextPageId();
    tmp->SetNextPageId(tmp_n->GetPageId());
    tmp_n->SetNextPageId(pid);
  } else {
    auto *tmp_n = reinterpret_cast<InternalPage *>(new_page_node);
    auto *tmp = reinterpret_cast<InternalPage *>(node);

    // move key & val pairs
    tmp->MoveHalfTo(tmp_n, buffer_pool_manager_);
  }

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
/*NOTE: when this is called, parent must hold latch*/
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  // ensure parent in transaction
  check_parent(old_node, transaction);

  // check
  if (old_node->GetPageId() == virtual_root_id_ || new_node->GetPageId() == virtual_root_id_) {
    LOG_DEBUG("parent_id: %d - sibling_id: %d, node_id: %d", old_node->GetParentPageId(), new_node->GetPageId(),
              old_node->GetPageId());
    {
      auto *page = fetch_page(root_page_id_);
      auto *page_node = reinterpret_cast<BPlusTreePage *>(page->GetData());
      ToString(page_node, buffer_pool_manager_);
    }
    throw Exception(ExceptionType::INVALID, "fatal - check - InsertIntoParent");
  }

  // root - terminate recursion
  if (old_node->IsRootPage()) {
    auto *root_page = new_rootL(false);

    // init new root (as internal)
    auto *root_node = reinterpret_cast<InternalPage *>(root_page->GetData());
    root_node->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);

    // adopt
    root_node->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);

    // done using; dirty
    buffer_pool_manager_->UnpinPage(root_node->GetPageId(), true);
    return;
  }

  // otherwise fetch parent page
  auto parent_id = old_node->GetParentPageId();
  auto *page = fetch_page(parent_id);  // latch is hold
  auto *parent_page_node = reinterpret_cast<InternalPage *>(page->GetData());

  // insert into parent, adopt
  auto new_size = parent_page_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());

  // recursive check
  if (new_size >= parent_page_node->GetMaxSize()) {
    auto *new_parent_page_node = Split(parent_page_node, transaction);
    auto partition_key = new_parent_page_node->KeyAt(0);  // partition key
    InsertIntoParent(parent_page_node, partition_key, new_parent_page_node, transaction);
    // buffer_pool_manager_->UnpinPage(new_parent_page_node->GetPageId(), true);
  }
  // close - mark dirty
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
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  mu_.lock();
  check_txns(transaction);

  // fetch - page hold WRITE latch -
  auto *page = WRITE_FindLeafPage(key, false, WType::DELETE, transaction);
  if (page == nullptr) {  // nullptr - empty - return immediately
    mu_.unlock();
    return;
  }
  auto *leaf_page_node = reinterpret_cast<LeafPage *>(page->GetData());

  // delete
  int original_size = leaf_page_node->GetSize();
  int remain_size = leaf_page_node->RemoveAndDeleteRecord(key, comparator_);
  bool has_modify = (original_size != remain_size);
  if (!has_modify) {
    release_N_unPin(page, transaction, false);  // page, ancestor not dirty - release and free
    mu_.unlock();
    return;
  }

  // redist or merge
  bool should_delete = false;
  if (remain_size < leaf_page_node->GetMinSize()) {
    should_delete = CoalesceOrRedistribute(leaf_page_node, transaction);
  }

  // close - and ancestor
  release_N_unPin(page, transaction, true);  // page, ancestor dirty - del will be addressed
  if (should_delete) {
    buffer_pool_manager_->DeletePage(leaf_page_node->GetPageId());
    // buffer_pool_manager_->info();
  }
  mu_.unlock();
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
/*when this is called, node and its parent has latch*/
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  // ensure parent in transaction
  check_parent(node, transaction);

  // root - termination of recursion
  if (node->IsRootPage()) {
    return AdjustRoot(node);
  }

  // get parent - and we must have its latch
  auto *parent_page = fetch_page(node->GetParentPageId());
  auto *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  int cur_index = parent_node->ValueIndex(node->GetPageId());

  // close - parent still in transaction
  buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);

  // get sibling - and latch
  auto *sibling_page = get_sibling(cur_index, parent_node);
  sibling_page->WLatch();
  transaction->AddIntoPageSet(sibling_page);
  auto *sibling_node = reinterpret_cast<N *>(sibling_page->GetData());

  /**
  NOTE: now node, sibling_node, parent_node are open - but node will be closed by caller
  NOTE: i.e. sibling_node and parent_node will be closed here
  redist - merge have different unpin strategy
  **/
  bool node_should_delete = false;
  if (sibling_node->GetSize() + node->GetSize() > node->GetMaxSize()) {
    // no recursion within callee
    Redistribute(sibling_node, node, cur_index);

  } else {
    // Coalesce has differnt move policy
    if (cur_index != 0) {
      node_should_delete = true;  // sibling <- me; delete me
    }

    // maybe recursion within callee
    bool parent_should_del = Coalesce(&sibling_node, &node, &parent_node, cur_index, transaction);

    // del
    if (parent_should_del) {  // need to del parent page here
      // LOG_DEBUG("addintodeletedpageset - parent_id: %d - sibling_id: %d, node_id: %d", parent_node->GetPageId(),
      //          sibling_node->GetPageId(), node->GetPageId());
      // LOG_DEBUG("parent pin: %d - sibling pin: %d", parent_page->GetPinCount(), sibling_page->GetPinCount());
      // LOG_DEBUG("root_id: %d - virtual_root_id: %d", root_page_id_, virtual_root_id_);
      // LOG_DEBUG("cur_index: %d", cur_index);
      // LOG_DEBUG("my parent id: %d, isLeaf: %d", node->GetParentPageId(), node->IsLeafPage());
      // for (auto &i : *(transaction->GetPageSet())) {
      //  auto *tmp_n = reinterpret_cast<BPlusTreePage *>(i->GetData());
      //  LOG_ERROR("check %d %d", tmp_n->GetPageId(), i->GetPageId());
      //}
      transaction->AddIntoDeletedPageSet(parent_node->GetPageId());
    }
    if (cur_index == 0) {  // depending on cur_index - either sibling or me need to del
      // LOG_DEBUG("del sibling id: %d - pin_count: %d - index %d", sibling_node->GetPageId(),
      // sibling_page->GetPinCount(),
      //          cur_index);
      transaction->AddIntoDeletedPageSet(sibling_node->GetPageId());
    }
  }
  return node_should_delete;
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
  auto isLeaf = (*node)->IsLeafPage();
  if (isLeaf) {
    // resolve leaf type
    //
    auto *tmp_n = reinterpret_cast<LeafPage *>(*neighbor_node);
    auto *tmp = reinterpret_cast<LeafPage *>(*node);

    // merge
    if (index == 0) {
      tmp_n->MoveAllTo(tmp);  // from sibling -> node
      (*parent)->Remove(1);   // inform parent
    } else {
      tmp->MoveAllTo(tmp_n);     // from node -> sibling
      (*parent)->Remove(index);  // inform parent
    }
  } else {
    // resolve internal page type
    //
    auto *tmp_n = reinterpret_cast<InternalPage *>(*neighbor_node);
    auto *tmp = reinterpret_cast<InternalPage *>(*node);

    // merge
    if (index == 0) {
      auto middle_key = (*parent)->KeyAt(1);
      tmp_n->MoveAllTo(tmp, middle_key, buffer_pool_manager_);  // from sibling -> node
      (*parent)->Remove(1);                                     // inform parent
    } else {
      auto middle_key = (*parent)->KeyAt(index);
      tmp->MoveAllTo(tmp_n, middle_key, buffer_pool_manager_);  // from node -> sibling
      (*parent)->Remove(index);                                 // inform parent
    }
  }

  // recursively check parent - now parent is 'leaf'
  if ((*parent)->GetSize() < (*parent)->GetMinSize()) {
    bool res = CoalesceOrRedistribute(*parent, transaction);

    //  // check
    //  if ((*parent)->GetPageId() == virtual_root_id_ || (*neighbor_node)->GetPageId() == virtual_root_id_) {
    //    LOG_DEBUG("coalesce");
    //    LOG_DEBUG("parent_id: %d - sibling_id: %d, node_id: %d", (*parent)->GetPageId(),
    //    (*neighbor_node)->GetPageId(),
    //              (*node)->GetPageId());
    //    LOG_DEBUG("cur_index: %d", index);
    //  }

    return res;
  }
  return false;  // if parent is fine, then do not delete parent
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
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  // get parent
  auto *parent_page = fetch_page(node->GetParentPageId());
  auto *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());

  bool isLeaf = node->IsLeafPage();
  if (isLeaf) {
    // resolve leaf type
    //
    auto *tmp_n = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(neighbor_node);
    auto *tmp = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(node);

    if (index == 0) {  // neighbor at my right
      parent_node->SetKeyAt(1, neighbor_node->KeyAt(1));
      tmp_n->MoveFirstToEndOf(tmp);
    } else {
      parent_node->SetKeyAt(index, neighbor_node->KeyAt(neighbor_node->GetSize() - 1));
      tmp_n->MoveLastToFrontOf(tmp);
    }
  } else {
    // resolve internal page type
    //
    auto *tmp_n = reinterpret_cast<InternalPage *>(neighbor_node);
    auto *tmp = reinterpret_cast<InternalPage *>(node);

    if (index == 0) {
      auto middle_key = parent_node->KeyAt(1);
      parent_node->SetKeyAt(1, neighbor_node->KeyAt(1));
      tmp_n->MoveFirstToEndOf(tmp, middle_key, buffer_pool_manager_);
    } else {
      auto middle_key = parent_node->KeyAt(index);
      parent_node->SetKeyAt(index, neighbor_node->KeyAt(neighbor_node->GetSize() - 1));
      tmp_n->MoveLastToFrontOf(tmp, middle_key, buffer_pool_manager_);
    }
  }
  // parent pin - 1, because we open again in this function
  buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 * if this is called, virtual root is hold; so it is safe to do any change
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  // case 2
  if (old_root_node->IsLeafPage()) {
    LOG_DEBUG("adj %d - %d - %d", old_root_node->GetPageId(), root_page_id_, old_root_node->GetSize());
    bool should_del = (old_root_node->GetSize() == 0);
    if (should_del) {
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId(false);
    }
    return should_del;
  }

  // internal page - if have more than one child - it is ok
  if (old_root_node->GetSize() > 1) {
    return false;
  }

  // case 1 - root has only one child - switch root node
  auto *tmp_old = reinterpret_cast<InternalPage *>(old_root_node);
  page_id_t val = tmp_old->RemoveAndReturnOnlyChild();

  LOG_DEBUG("switch from: %d - to: %d", root_page_id_, val);

  // switch root to its only child
  auto *page = fetch_page(val);
  auto *tmp = reinterpret_cast<BPlusTreePage *>(page->GetData());
  tmp->SetParentPageId(INVALID_PAGE_ID);

  if (tmp->GetPageId() != page->GetPageId() || page->GetPageId() != val) {
    LOG_DEBUG("adjustroot %d - %d - %d", tmp->GetPageId(), page->GetPageId(), val);
  }

  // switch
  root_page_id_ = val;
  UpdateRootPageId(false);

  // mark dirty
  buffer_pool_manager_->UnpinPage(val, true);

  return true;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  // return INDEXITERATOR_TYPE();
  mu_.lock();
  KeyType k;
  auto *page = READ_FindLeafPage(k, true);
  if (page == nullptr) {
    mu_.unlock();
    return INDEXITERATOR_TYPE(nullptr, buffer_pool_manager_, -1);
  }
  auto *leaf_page_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
  mu_.unlock();
  return INDEXITERATOR_TYPE(leaf_page_node, buffer_pool_manager_, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  // return INDEXITERATOR_TYPE();
  mu_.lock();

  auto *page = READ_FindLeafPage(key, false);
  if (page == nullptr) {
    mu_.unlock();
    return INDEXITERATOR_TYPE(nullptr, buffer_pool_manager_, -1);
  }
  auto *leaf_page_node = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
  mu_.unlock();
  return INDEXITERATOR_TYPE(leaf_page_node, buffer_pool_manager_, leaf_page_node->KeyIndex(key, comparator_));
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() { return INDEXITERATOR_TYPE(nullptr, buffer_pool_manager_, -1); }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::new_page(page_id_t *pid) {
  auto *page = buffer_pool_manager_->NewPage(pid);
  if (page == nullptr) {
    LOG_DEBUG("%d %d", virtual_root_id_, root_page_id_);
    {
      Page *page;
      BPlusTreePage *page_node;

      page = buffer_pool_manager_->FetchPage(root_page_id_);
      page_node = reinterpret_cast<BPlusTreePage *>(page->GetData());
      ToString(page_node, buffer_pool_manager_);
    }
    throw Exception(ExceptionType::INVALID, "new page");
  }
  if (page->GetPageId() != *pid) {
    LOG_DEBUG("new_page %d - %d", *pid, page->GetPageId());
    throw Exception(ExceptionType::INVALID, "new page");
  }
  return page;
}
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::fetch_page(page_id_t pid) {
  auto *page = buffer_pool_manager_->FetchPage(pid);
  if (page->GetPageId() != pid) {
    LOG_DEBUG("fetch_page %d - %d", pid, page->GetPageId());
    throw Exception(ExceptionType::INVALID, "new page");
  }
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "out of mem");
  }
  return page;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::check_parent(BPlusTreePage *node, Transaction *transaction) {
  page_id_t parent_id;
  if (node->IsRootPage()) {
    parent_id = virtual_root_id_;
  } else {
    parent_id = node->GetParentPageId();
  }
  if (!is_pid_in_txns(transaction, parent_id)) {
    LOG_ERROR("check parent %d %d %d", parent_id, node->GetPageId(), node->GetParentPageId());
    throw Exception(ExceptionType::INVALID, "check_parent");
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::is_pid_in_txns(Transaction *transaction, page_id_t pid) {
  std::shared_ptr<std::deque<Page *>> page_set = transaction->GetPageSet();
  for (auto &i : *page_set) {
    auto *tmp = reinterpret_cast<BPlusTreePage *>(i->GetData());
    if (tmp->GetPageId() != i->GetPageId()) {
      LOG_ERROR("is_pid_in_txns %d %d", tmp->GetPageId(), i->GetPageId());
    }
    if (tmp->GetPageId() == pid) {
      return true;
    }
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::check_txns(Transaction *transaction) {
  if (!transaction->GetPageSet()->empty()) {
    throw Exception(ExceptionType::INVALID, "check_txns");
  }
  if (!transaction->GetDeletedPageSet()->empty()) {
    throw Exception(ExceptionType::INVALID, "check_txns");
  }
}

// get sibling - if node is leftmost, get right sibling - otherwise get left sibling
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::get_sibling(int index, InternalPage *parent_node) {
  Page *sibling_page;
  page_id_t sib_id;
  if (index == 0) {
    sib_id = parent_node->ValueAt(1);
  } else {
    sib_id = parent_node->ValueAt(index - 1);
  }
  sibling_page = fetch_page(sib_id);
  return sibling_page;
}

/* abstract method to lock and unlock virtual root */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::lock() {
  auto *page = fetch_page(virtual_root_id_);
  page->WLatch();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::unlock() {
  auto *page = fetch_page(virtual_root_id_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(virtual_root_id_, false);

  // close the one from lock
  buffer_pool_manager_->UnpinPage(virtual_root_id_, false);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::release_N_unPin(Page *page, Transaction *transaction, bool dirty) {
  free_ancestor(transaction, dirty);  // ancestor must be dirty, otherwise it won't be in transaction
  auto *tmp = reinterpret_cast<BPlusTreePage *>(page->GetData());
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(tmp->GetPageId(), dirty);
}

INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::new_rootL(bool new_tree) {
  auto *page = new_page(&root_page_id_);
  UpdateRootPageId(new_tree);  // insert header page (meta data) - true for start a new tree
  return page;
}

// @return: page with read latch
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::READ_FindLeafPage(const KeyType &key, bool leftMost, Transaction *transaction) {
  // virtual root
  Page *page;
  Page *childPage;
  BPlusTreePage *page_node;
  page_id_t val;

  page = fetch_page(virtual_root_id_);
  page->WLatch();
  page_node = reinterpret_cast<BPlusTreePage *>(page->GetData());

  if (IsEmpty()) {
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page_node->GetPageId(), false);
    return nullptr;
  }

  // if  root and leaf - new tree - return directly
  // if root ok, then recursively search
  while (!page_node->IsLeafPage() || page_node->GetPageId() == virtual_root_id_) {
    if (page_node->GetPageId() == virtual_root_id_) {
      val = root_page_id_;
    } else {
      auto *internal_page_node = reinterpret_cast<InternalPage *>(page_node);
      val = (leftMost) ? internal_page_node->ValueAt(0) : internal_page_node->Lookup(key, comparator_);
    }

    // get child
    childPage = fetch_page(val);
    childPage->RLatch();

    // unpin current page and find next
    if (page_node->GetPageId() == virtual_root_id_) {
      page->WUnlatch();
    } else {
      page->RUnlatch();
    }
    buffer_pool_manager_->UnpinPage(page_node->GetPageId(), false);

    // swap
    page = childPage;
    page_node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }
  if (page == nullptr) {
    throw Exception(ExceptionType::INVALID, "traverse");
  }
  return page;
}

INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::WRITE_FindLeafPage_new(const KeyType &key, const ValueType &value, bool leftMost, WType op,
                                             Transaction *transaction) {
  Page *page;
  Page *childPage;
  BPlusTreePage *page_node;
  page_id_t val;

  page = fetch_page(virtual_root_id_);
  page->WLatch();
  page_node = reinterpret_cast<BPlusTreePage *>(page->GetData());

  if (IsEmpty()) {
    auto k = key;
    auto v = value;
    StartNewTree(k, v);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page_node->GetPageId(), false);
    return nullptr;
  }

  // traverse
  while (!page_node->IsLeafPage() || page_node->GetPageId() == virtual_root_id_) {
    if (page_node->GetPageId() == virtual_root_id_) {
      val = root_page_id_;
    } else {
      auto *internal_page_node = reinterpret_cast<InternalPage *>(page_node);
      val = (leftMost) ? internal_page_node->ValueAt(0) : internal_page_node->Lookup(key, comparator_);
    }

    // get child
    childPage = fetch_page(val);
    childPage->WLatch();

    // add current page to transactions
    transaction->AddIntoPageSet(page);

    // traverse
    page = childPage;
    page_node = reinterpret_cast<BPlusTreePage *>(page->GetData());

    // check
    if (isSafe(op, page_node)) {
      free_ancestor(transaction, false);
    }
  }
  if (page == nullptr) {
    throw Exception(ExceptionType::INVALID, "traverse");
  }
  return page;
}

// @return: leaf with write latch
// if parents are not safe, they exist in transaction
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::WRITE_FindLeafPage(const KeyType &key, bool leftMost, WType op, Transaction *transaction) {
  // get virtual root
  // if update can potentially affect root
  // virtual root's wlatch is held
  Page *page;
  Page *childPage;
  BPlusTreePage *page_node;
  page_id_t val;

  page = fetch_page(virtual_root_id_);
  page->WLatch();
  page_node = reinterpret_cast<BPlusTreePage *>(page->GetData());

  if (IsEmpty()) {
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page_node->GetPageId(), false);
    return nullptr;
  }

  // traverse
  while (!page_node->IsLeafPage() || page_node->GetPageId() == virtual_root_id_) {
    if (page_node->GetPageId() == virtual_root_id_) {
      val = root_page_id_;
    } else {
      auto *internal_page_node = reinterpret_cast<InternalPage *>(page_node);
      val = (leftMost) ? internal_page_node->ValueAt(0) : internal_page_node->Lookup(key, comparator_);
    }

    // get child
    childPage = fetch_page(val);
    childPage->WLatch();

    // add current page to transactions
    transaction->AddIntoPageSet(page);

    // traverse
    page = childPage;
    page_node = reinterpret_cast<BPlusTreePage *>(page->GetData());

    // check
    if (isSafe(op, page_node)) {
      free_ancestor(transaction, false);
    }
  }
  if (page == nullptr) {
    throw Exception(ExceptionType::INVALID, "traverse");
  }
  return page;
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::isSafe(WType op, BPlusTreePage *node) {
  if (op == WType::INSERT && node->GetSize() < node->GetMaxSize() - 1) {
    return true;
  }
  if (op == WType::DELETE && node->GetSize() > node->GetMinSize() + 1) {  // or node->GetMinSize() + 1
    return true;
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::free_ancestor(Transaction *transaction, bool ancestor_dirty) {
  // this is a pointer
  auto page_set = transaction->GetPageSet();

  while (!page_set->empty()) {
    Page *p = page_set->back();
    auto *tmp = reinterpret_cast<BPlusTreePage *>(p->GetData());
    page_id_t pid = tmp->GetPageId();

    p->WUnlatch();
    if (pid == virtual_root_id_) {
      buffer_pool_manager_->UnpinPage(pid, false);
    } else {
      buffer_pool_manager_->UnpinPage(pid, ancestor_dirty);
    }

    // if in del set, del
    if (transaction->GetDeletedPageSet()->find(pid) != transaction->GetDeletedPageSet()->end()) {
      // for (auto &i : *page_set) {
      //  auto *tmp_n = reinterpret_cast<BPlusTreePage *>(i->GetData());
      //  LOG_ERROR("delset - check %d %d", tmp_n->GetPageId(), i->GetPageId());
      //}
      // LOG_DEBUG("delset - %d ", pid);
      buffer_pool_manager_->DeletePage(pid);
      // buffer_pool_manager_->info();
      transaction->GetDeletedPageSet()->erase(pid);
    }
    // notice this clears elem in transaction - because page_set is a pointer
    page_set->pop_back();
  }

  // check
  if (!page_set->empty() || !transaction->GetPageSet()->empty()) {
    throw Exception(ExceptionType::INVALID, "del");
  }
  if (!transaction->GetDeletedPageSet()->empty()) {
    LOG_DEBUG("fatal - GetDeletedPageSet");
    auto p = transaction->GetDeletedPageSet();
    for (auto &i : *p) {
      LOG_DEBUG("%d", i);
    }
    throw Exception(ExceptionType::INVALID, "del");
  }
}

/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  // throw Exception(ExceptionType::NOT_IMPLEMENTED, "Implement this for test");
  LOG_ERROR("only used for checkpoint 1");

  // protect root
  if (IsEmpty()) {
    return nullptr;
  }

  Page *page;
  BPlusTreePage *page_node;
  page_id_t val;

  // root
  page = buffer_pool_manager_->FetchPage(root_page_id_);

  page_node = reinterpret_cast<BPlusTreePage *>(page->GetData());

  // if  root and leaf - new tree - return directly
  // if root ok, then recursively search
  while (!page_node->IsLeafPage()) {
    // data key must exist in internal node
    auto *internal_page_node = reinterpret_cast<InternalPage *>(page_node);

    val = (leftMost) ? internal_page_node->ValueAt(0) : internal_page_node->Lookup(key, comparator_);

    // unpin current page and find next
    buffer_pool_manager_->UnpinPage(page_node->GetPageId(), false);
    page = buffer_pool_manager_->FetchPage(val);
    page_node = reinterpret_cast<BPlusTreePage *>(page->GetData());
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
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print() {
  if (root_page_id_ != INVALID_PAGE_ID) {
    auto *page = fetch_page(root_page_id_);
    auto *tmp = reinterpret_cast<BPlusTreePage *>(page->GetData());
    ToString(tmp, buffer_pool_manager_);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  } else {
    LOG_INFO("empty page");
  }
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
