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
// Created by WangYunlai on 2022/6/7.
//

#pragma once

#include "common/sys/rc.h"
#include "sql/operator/physical_operator.h"
#include "storage/record/record_manager.h"
#include "common/types.h"

class Table;

/**
 * @brief 表扫描物理算子
 * @ingroup PhysicalOperator
 */
class TableScanPhysicalOperator : public PhysicalOperator
{
public:
  TableScanPhysicalOperator(Table *table, ReadWriteMode mode) : table_(table), mode_(mode) {}

  virtual ~TableScanPhysicalOperator() = default;

  string param() const override;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::TABLE_SCAN; }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  Tuple *current_tuple() override;

  void set_predicates(vector<unique_ptr<Expression>> &&exprs);

  void               set_table_alias(const std::string &table_alias) { table_alias_ = table_alias; }
  const std::string &table_alias() const { return table_alias_; }

private:
  RC filter(RowTuple &tuple, bool &result);

private:
  Table                         *table_ = nullptr;
  Trx                           *trx_   = nullptr;
  ReadWriteMode                  mode_  = ReadWriteMode::READ_WRITE;
  RecordFileScanner              record_scanner_;
  Record                         current_record_;
  RowTuple                       tuple_;
  vector<unique_ptr<Expression>> predicates_;  // TODO chang predicate to table tuple filter

  string table_alias_;
};
