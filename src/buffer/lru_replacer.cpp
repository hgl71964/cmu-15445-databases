//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages)
  :num_pages(num_pages)
  {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  return false;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  if (map.find(frame_id) == map.end()) {
    ;
  } else {
    // remove from linked list and map
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  if (map.find(frame_id) == map.end()) {
    ;
  } else {
    // add to linked list and map
  }
}

size_t LRUReplacer::Size() { return lst.size(); }

}  // namespace bustub
