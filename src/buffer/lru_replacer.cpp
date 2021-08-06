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
#include <cstddef>
#include <iterator>
#include "common/config.h"
#include <assert.h>

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages)
  :num_pages(num_pages)
  {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {

  auto ok = !lst.empty();

  if (ok) {
    // retrieve lru
    *frame_id = lst.front();

    // remove from replacer
    lst.erase(lst.begin());
    map.erase(*frame_id);
  }
  return ok;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  if (map.find(frame_id) != map.end()) {

    lst.erase(map[frame_id]);
    map.erase(frame_id);
    }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {

  // frame_id_t has to be unique
  if (map.find(frame_id) == map.end()) {

    assert(this->Size() < num_pages); // check

    // add to linked list and map
    lst.push_back(frame_id);
    map[frame_id] = lst.rbegin();
  }
}

size_t LRUReplacer::Size() { return static_cast<size_t>(lst.size()); }

}  // namespace bustub
