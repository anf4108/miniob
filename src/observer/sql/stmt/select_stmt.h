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
// Created by Wangyunlai on 2022/6/5.
//

#pragma once

#include "common/sys/rc.h"
#include "sql/stmt/stmt.h"
#include "storage/field/field.h"

class FieldMeta;
class FilterStmt;
class Db;
class Table;

/**
 * @brief 表示select语句
 * @ingroup Statement
 */
class SelectStmt : public Stmt
{
public:
  SelectStmt() = default;
  ~SelectStmt() override;

  StmtType type() const override { return StmtType::SELECT; }

public:
  static RC create(Db *db, SelectSqlNode &select_sql, Stmt *&stmt,
      std::shared_ptr<std::unordered_map<string, string>> name2alias            = nullptr,
      std::shared_ptr<std::unordered_map<string, string>> alias2name            = nullptr,
      std::shared_ptr<std::vector<string>>                loaded_relation_names = nullptr);

  /// 用于实际执行时，转换表达式中的别名为表名，检索storage中的字段
  static RC convert_alias_to_name(Expression *expr, std::shared_ptr<std::unordered_map<string, string>> alias2name);

public:
  const vector<Table *>          &tables() const { return tables_; }
  FilterStmt                     *filter_stmt() const { return filter_stmt_; }
  vector<unique_ptr<Expression>> &query_expressions() { return query_expressions_; }
  vector<unique_ptr<Expression>> &group_by() { return group_by_; }
  vector<string>                 &table_aliases() { return table_aliases_; }

private:
  vector<unique_ptr<Expression>> query_expressions_;
  vector<Table *>                tables_;
  vector<string>                 table_aliases_;
  FilterStmt                    *filter_stmt_ = nullptr;
  vector<unique_ptr<Expression>> group_by_;
};
