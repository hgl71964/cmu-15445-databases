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
#include <mutex>
#include "common/config.h"
#include <assert.h>

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages)
  :num_pages(num_pages)
  {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {

  std::scoped_lock<std::mutex> lock(mu);

  auto ok = !lst.empty();

  if (ok) {
    // retrieve lru
    *frame_id = lst.front();

    // remove from replacer
    lst.erase(lst.begin());
    map.erase(*frame_id);
  } else {
    frame_id = nullptr;
  }

  return ok;
}

void LRUReplacer::Pin(frame_id_t frame_id) {

  std::scoped_lock<std::mutex> lock(mu);
  if (map.find(frame_id) != map.end()) {
    lst.erase(map[frame_id]);
    map.erase(frame_id);
    }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {

  std::scoped_lock<std::mutex> lock(mu);

  // frame_id_t has to be unique
  if (map.find(frame_id) == map.end()) {

    assert(lst.size() < num_pages); // check

    lst.push_back(frame_id);

    std::list<frame_id_t>::iterator iter = lst.end();
    iter--;
    map[frame_id] = iter;
  }
}

size_t LRUReplacer::Size() {
  std::scoped_lock<std::mutex> lock(mu);
  size_t res = static_cast<size_t>(lst.size());
  return res;
}

}  // namespace bustub
