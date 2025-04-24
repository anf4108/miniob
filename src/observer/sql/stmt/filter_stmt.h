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

#pragma once

#include "common/lang/unordered_map.h"
#include "common/lang/vector.h"
#include "sql/expr/expression.h"
#include "sql/parser/parse_defs.h"
#include "sql/stmt/stmt.h"

class Db;
class Table;
class FieldMeta;

// remove all the unnecessary filter units, replaced by Expression

/**
 * @brief Filter/谓词/过滤语句
 * @ingroup Statement
 */
class FilterStmt
{
public:
  FilterStmt() = default;
  virtual ~FilterStmt();

public:
  vector<unique_ptr<Expression>>       &conditions() { return conditions_; }
  const vector<unique_ptr<Expression>> &conditions() const { return conditions_; }
  vector<char>                         &conjunction_types() { return conjunction_types_; }

public:
  static RC create(Db *db, Table *default_table, std::unordered_map<std::string, Table *> *tables,
      std::vector<ConditionSqlNode> &conditions, FilterStmt *&stmt);

private:
  vector<unique_ptr<Expression>> conditions_;
  vector<char>                   conjunction_types_;  // 发现复杂子查询中需要支持 AND or OR, 现在进行扩展
};
