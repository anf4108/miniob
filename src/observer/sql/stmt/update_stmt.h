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

#include "common/sys/rc.h"
#include "common/value.h"
#include "common/lang/memory.h"
#include "sql/stmt/stmt.h"
#include "storage/field/field.h"
#include "sql/stmt/filter_stmt.h"
#include "sql/expr/expression.h"

class Table;

/**
 * @brief 更新语句
 * @ingroup Statement
 */
class UpdateStmt : public Stmt
{
public:
  UpdateStmt() = default;
  UpdateStmt(Table *table, vector<FieldMeta> attrs, vector<unique_ptr<Expression>> exprs, FilterStmt *stmt);

public:
  static RC create(Db *db, const UpdateSqlNode &update_sql, Stmt *&stmt);
  StmtType  type() const override { return StmtType::UPDATE; }

public:
  const vector<FieldMeta>        &field_metas() { return field_metas_; }
  vector<unique_ptr<Expression>> &exprs() { return exprs_; }
  FilterStmt                     *filter_stmt() { return filter_stmt_; }
  Table                          *table() { return table_; }

private:
  Table                         *table_ = nullptr;
  vector<FieldMeta>              field_metas_;
  vector<unique_ptr<Expression>> exprs_;
  FilterStmt                    *filter_stmt_;
};
