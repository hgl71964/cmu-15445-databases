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
#include <mutex>
#include <unordered_map>
#include "common/config.h"
#include "common/logger.h"
#include "storage/page/page.h"

namespace bustub {

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

  // TODO add lock

  if (page_table_.find(page_id) != page_table_.end()) {
    // pin + get frame id
    frame_id_t frame_id = page_table_[page_id];
    replacer_->Pin(frame_id);
    page_meta_data[page_id] += 1;

    return &pages_[frame_id];

  } else {
  }

  return nullptr;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) { return false; }

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  return false;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  std::scoped_lock<std::mutex> lock(latch_);

  // 0.
  page_id_t new_page_id = disk_manager_->AllocatePage();

  // 1.
  bool all_pin = true;
  for (size_t i = 0; i< pool_size_; i++) {
    if (pages_[i].pin_count_ < 1) {
      all_pin = false;
      break;
    }
  }
  if (all_pin) {
    return nullptr;
  }

  // 2.
  frame_id_t new_frame_id;
  if (!free_list_.empty()) {
    new_frame_id = free_list_.front();
    free_list_.pop_front();

  } else {  // find in lru
    auto ok = replacer_->Victim(&new_frame_id);
    if (!ok) {
      LOG_ERROR("NewPageImpl replacer not ok");
    }
  }

  // 3.
  Reset_meta_dataL(new_frame_id);
  pages_[new_frame_id].page_id_ = new_page_id;
  pages_[new_frame_id].pin_count_ = 0; // XXX init pin count as 0? other meta data?

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

  // 0.
  disk_manager_->DeallocatePage(page_id);

  // 1.
  if (page_table_.find(page_id) == page_table_.end()) return true;

  // 2.
  if (pages_[page_table_[page_id]].pin_count_ > 0) return false;

  // 3.
  int free_frame_id = page_table_[page_id];
  page_table_.erase(page_id);

  // reset meta data, XXX need to flush if dirty??
  Reset_meta_dataL(free_frame_id);

  // return to free list
  free_list_.push_back(free_frame_id);

  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
}

void BufferPoolManager::Reset_meta_dataL(frame_id_t frame_id) {
  pages_[frame_id].ResetMemory();
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].page_id_ = -1;
}

}  // namespace bustub
