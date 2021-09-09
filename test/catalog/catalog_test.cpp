//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// catalog_test.cpp
//
// Identification: test/catalog/catalog_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdint>
#include <iostream>
#include <string>
#include <unordered_set>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "catalog/catalog.h"
#include "gtest/gtest.h"
#include "type/value_factory.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(CatalogTest, CreateTableTest) {
  auto disk_manager = new DiskManager("catalog_test.db");
  auto bpm = new BufferPoolManager(32, disk_manager);
  auto catalog = new Catalog(bpm, nullptr, nullptr);
  std::string table_name = "potato";

  // The table shouldn't exist in the catalog yet.
  EXPECT_THROW(catalog->GetTable(table_name), std::out_of_range);

  // Put the table into the catalog.
  std::vector<Column> columns;
  columns.emplace_back("A", TypeId::INTEGER);
  columns.emplace_back("B", TypeId::BOOLEAN);

  Schema schema(columns);
  auto *table_metadata = catalog->CreateTable(nullptr, table_name, schema);
  (void)table_metadata;

  // Notice that this test case doesn't check anything! :(
  // It is up to you to extend it

  delete catalog;
  delete bpm;
  delete disk_manager;
}

// NOLINTNEXTLINE
TEST(CatalogTest, CreateIndexTest) {
  auto disk_manager = new DiskManager("catalog_test.db");
  auto bpm = new BufferPoolManager(32, disk_manager);
  auto catalog = new Catalog(bpm, nullptr, nullptr);
  std::string table_name = "potato";

  // Put the table into the catalog.
  std::vector<Column> columns;
  columns.emplace_back("A", TypeId::INTEGER);
  columns.emplace_back("B", TypeId::BOOLEAN);

  Schema schema(columns);
  auto *table_metadata = catalog->CreateTable(nullptr, table_name, schema);
  (void)table_metadata;

  // index
  std::vector<Column> keys;
  keys.emplace_back("K", TypeId::INTEGER);

  Schema key_schema(keys);
  std::string index_name = "tomato";

  auto keys_attr = std::vector<uint32_t>{1};
  size_t keysize = 1;
  auto *idx_info = catalog->CreateIndex<GenericKey<4>, RID, GenericComparator<4>>(
      nullptr, index_name, table_name, schema, key_schema, keys_attr, keysize);

  std::cout << idx_info->key_size_ << std::endl;
  // std::cout << idx_info->key_schema_ << std::endl;

  delete catalog;
  delete bpm;
  delete disk_manager;
}

}  // namespace bustub
