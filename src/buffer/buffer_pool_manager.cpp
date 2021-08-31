//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <list>
#include <unordered_map>
#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "storage/page/page.h"

namespace bustub {
namespace {
auto debug_msg = false;
}

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  std::scoped_lock<std::mutex> lock(latch_);

  check();

  // 1.1
  if (page_table_.find(page_id) != page_table_.end()) {
    frame_id_t frame_id = page_table_[page_id];
    replacer_->Pin(frame_id);
    pages_[frame_id].pin_count_ += 1;

    if (pages_[frame_id].GetPageId() != page_id) {
      LOG_ERROR("FetchPageImpl fatal - %d - %d - page_table_ is not up-to-date", pages_[frame_id].GetPageId(), page_id);
    }
    return &pages_[frame_id];
  }

  // if all pinned, cannot find replacement
  if (is_all_pin()) {
    return nullptr;
  }

  // 1.2
  auto frame_id = Find_replacementL();

  // 2.
  if (pages_[frame_id].is_dirty_) {
    disk_manager_->WritePage(pages_[frame_id].page_id_, pages_[frame_id].GetData());
  }

  // 3.
  auto old_page_id = pages_[frame_id].page_id_;
  if (page_table_.find(old_page_id) != page_table_.end()) {
    LOG_DEBUG("%d - size %ld", page_table_.find(old_page_id) != page_table_.end(), page_table_.size());
    page_table_.erase(old_page_id);
    LOG_DEBUG("%d - size %ld", page_table_.find(old_page_id) != page_table_.end(), page_table_.size());
  }
  page_table_[page_id] = frame_id;

  // 4.
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 1;
  replacer_->Pin(frame_id);
  disk_manager_->ReadPage(page_id, pages_[frame_id].data_);

  if (debug_msg) {
    LOG_INFO("FetchPageImpl - not found - page_id: %d", page_id);
  }

  return &pages_[frame_id];
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  std::scoped_lock<std::mutex> lock(latch_);

  check();

  // does not exist
  if (page_table_.find(page_id) == page_table_.end()) {
    LOG_ERROR("unpin page_id: %d", page_id);
    return true;
  }
  if (pages_[page_table_[page_id]].pin_count_ <= 0) {
    // LOG_INFO("over-unpin page_id: %d - pid: %d - pin count %d  ", page_id, pages_[page_table_[page_id]].GetPageId(),
    //         pages_[page_table_[page_id]].pin_count_);
    return false;
  }

  if (pages_[page_table_[page_id]].GetPageId() != page_id) {
    LOG_ERROR("unpin %d -  %d", page_id, pages_[page_table_[page_id]].GetPageId());
    return false;
  }

  pages_[page_table_[page_id]].is_dirty_ |= is_dirty;
  pages_[page_table_[page_id]].pin_count_ -= 1;

  if (pages_[page_table_[page_id]].pin_count_ < 1) {
    replacer_->Unpin(page_table_[page_id]);

    if (debug_msg) {
      LOG_INFO("Enter Replacer page_id: %d", page_id);
    }
  }

  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::scoped_lock<std::mutex> lock(latch_);

  if (page_id == INVALID_PAGE_ID) {
    LOG_ERROR("flush page INVALID_PAGE_ID");
    return false;
  }
  if (page_id != pages_[page_table_[page_id]].page_id_) {
    LOG_ERROR("flush page not equal; maybe race");
    return false;
  }
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }

  if (debug_msg) {
    LOG_INFO("flush page_id: %d, pin count: %d", page_id, pages_[page_table_[page_id]].pin_count_);
  }
  disk_manager_->WritePage(page_id, pages_[page_table_[page_id]].GetData());

  // after flush, dirty = false
  pages_[page_table_[page_id]].is_dirty_ = false;
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  std::scoped_lock<std::mutex> lock(latch_);

  check();

  // 0.
  page_id_t new_page_id = disk_manager_->AllocatePage();

  // 1.
  if (is_all_pin()) {
    return nullptr;
  }

  // 2. (now must have unpinned pages)
  if (debug_msg) {
    LOG_INFO("NewPageImpl - new page_id: %d", new_page_id);
  }
  frame_id_t new_frame_id = Find_replacementL();

  // delete from page table
  page_id_t old_pid = pages_[new_frame_id].page_id_;
  if (page_table_.find(old_pid) != page_table_.end()) {
    LOG_DEBUG("%d - size %ld", page_table_.find(old_pid) != page_table_.end(), page_table_.size());
    page_table_.erase(old_pid);
    LOG_DEBUG("%d - size %ld", page_table_.find(old_pid) != page_table_.end(), page_table_.size());
  }

  // flush if dirty
  if (pages_[new_frame_id].is_dirty_) {
    disk_manager_->WritePage(old_pid, pages_[new_frame_id].GetData());
  }

  // 3.
  pages_[new_frame_id].ResetMemory();
  pages_[new_frame_id].is_dirty_ = false;
  pages_[new_frame_id].page_id_ = new_page_id;
  pages_[new_frame_id].pin_count_ = 1;
  replacer_->Pin(new_frame_id);

  // register in page table
  page_table_[new_page_id] = new_frame_id;

  // 4.
  *page_id = new_page_id;
  return &pages_[new_frame_id];
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.

  std::scoped_lock<std::mutex> lock(latch_);

  check();

  // 0.
  disk_manager_->DeallocatePage(page_id);

  // 1.
  if (page_table_.find(page_id) == page_table_.end()) {
    LOG_INFO("bpm - not find - %d", page_id);
    return true;
  }

  // 2.
  if (pages_[page_table_[page_id]].pin_count_ > 0) {
    LOG_DEBUG("try to del page id: %d - but pin_count: %d", page_id, pages_[page_table_[page_id]].pin_count_);
    return false;
  }

  // 3.
  frame_id_t free_frame_id = page_table_[page_id];
  page_id_t pid = page_id;
  if (page_table_.find(pid) != page_table_.end()) {
    LOG_DEBUG("%d - size %ld", page_table_.find(pid) != page_table_.end(), page_table_.size());
    page_table_.erase(pid);
    LOG_DEBUG("%d - size %ld", page_table_.find(pid) != page_table_.end(), page_table_.size());
  }

  // LOG_INFO("bpm - update %d - %d", page_id, free_frame_id);

  // reset meta data, flush if dirty
  if (pages_[free_frame_id].is_dirty_) {
    disk_manager_->WritePage(page_id, pages_[free_frame_id].GetData());
  }

  if (debug_msg) {
    LOG_INFO("DeletePageImpl - page_id: %d", page_id);
  }
  Reset_meta_dataL(free_frame_id);

  // LOG_INFO("bpm - after update %d - %d - %d", page_id, pages_[free_frame_id].GetPageId(),
  //         pages_[free_frame_id].GetPinCount());

  // return to free list
  free_list_.push_back(free_frame_id);

  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!

  std::scoped_lock<std::mutex> lock(latch_);

  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].page_id_ == INVALID_PAGE_ID) {
      continue;
    }
    disk_manager_->WritePage(pages_[i].page_id_, pages_[i].GetData());
    pages_[i].is_dirty_ = false;
  }
}

bool BufferPoolManager::is_all_pin() {
  bool all_pin = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ < 1) {
      all_pin = false;
      break;
    }
  }
  return all_pin;
}

void BufferPoolManager::Reset_meta_dataL(frame_id_t frame_id) {
  pages_[frame_id].ResetMemory();
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
}

frame_id_t BufferPoolManager::Find_replacementL() {
  frame_id_t fid;

  // always find in free list first
  if (!free_list_.empty()) {
    fid = free_list_.front();
    free_list_.pop_front();
  } else {
    // then find in lru
    auto ok = replacer_->Victim(&fid);
    if (!ok) {
      LOG_ERROR("find replacement replacer not ok");  // XXX
    }

    if (debug_msg) {
      LOG_INFO("Victim - page_id: %d", pages_[fid].page_id_);
    }
  }
  return fid;
}
void BufferPoolManager::check() {
  for (auto &it : page_table_) {
    auto pid = it.first;
    auto frame_id = it.second;
    if (pages_[frame_id].GetPageId() != pid) {
      LOG_ERROR("check %d - %d - %d", pid, pages_[frame_id].GetPageId(), frame_id);
      throw Exception(ExceptionType::INVALID, "bpm check");
    }
  }
}

}  // namespace bustub
