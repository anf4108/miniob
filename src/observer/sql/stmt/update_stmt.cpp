/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2022/5/22.
//

#include <unordered_map>
#include "common/log/log.h"
#include "common/value.h"
#include "sql/stmt/update_stmt.h"
#include "sql/stmt/filter_stmt.h"
#include "storage/db/db.h"
#include "storage/table/table.h"
#include "sql/expr/expression.h"

UpdateStmt::UpdateStmt(Table *table, Field field, Value value, FilterStmt *filter_stmt)
    : table_(table), field_(field), value_(value), filter_stmt_(filter_stmt)
{}

RC UpdateStmt::create(Db *db, const UpdateSqlNode &update, Stmt *&stmt)
{
  RC          rc         = RC::SUCCESS;
  const char *table_name = update.relation_name.c_str();
  if (nullptr == db || nullptr == table_name) {
    LOG_WARN("invalid argument. db=%p, table_name=%p", db, table_name);
    return RC::INVALID_ARGUMENT;
  }

  // check whether the table exists
  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  const FieldMeta *field_meta;
  field_meta = table->table_meta().field(update.attribute_name.c_str());
  if (field_meta == nullptr) {
    LOG_WARN("field %s not exist in table %s", update.attribute_name.c_str(), update.relation_name.c_str());
    return RC::SCHEMA_FIELD_NOT_EXIST;
  }

  std::unordered_map<string, Table *> table_map;
  table_map.emplace(string(table_name), table);

  Value value = Value(update.value);

  FilterStmt *filter_stmt = nullptr;
  rc =
      FilterStmt::create(db, table, &table_map, const_cast<vector<ConditionSqlNode> &>(update.conditions), filter_stmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create filter statement. rc=%d:%s", rc, strrc(rc));
    return rc;
  }

  if (field_meta->type() != value.attr_type()) {
    // TODO: 类型转换 (value.cpp)
    // if (!Value::convert(value.attr_type(), field_meta->type(), value)) {
    // }
    if (value.attr_type() == AttrType::NULLS) {
      // field_meta
    } else {
      LOG_WARN("update value cannot convert into target type, src=%s, target=%s",
                 attr_type_to_string(value.attr_type()), attr_type_to_string(field_meta->type()));
      return RC::SCHEMA_FIELD_TYPE_MISMATCH;
    }
  }

  // 创建 UpdateStmt 对象
  Field field = Field(table, field_meta);

  stmt = new UpdateStmt(table, field, value, filter_stmt);
  return RC::SUCCESS;
}
