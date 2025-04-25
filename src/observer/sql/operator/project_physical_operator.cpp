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
// Created by WangYunlai on 2022/07/01.
//

#include "sql/operator/project_physical_operator.h"
#include "common/log/log.h"
#include "storage/record/record.h"
#include "storage/table/table.h"

using namespace std;

ProjectPhysicalOperator::ProjectPhysicalOperator(vector<unique_ptr<Expression>> &&expressions)
    : expressions_(std::move(expressions)), tuple_(expressions_)
{}

RC ProjectPhysicalOperator::open(Trx *trx)
{
  if (children_.empty()) {
    no_child = true;
    return RC::SUCCESS;
  }
  PhysicalOperator *child = children_[0].get();

  if (outer_tuple != nullptr) {
    LOG_DEBUG("msg from project_phy_oper: we are in subquery");
    child->set_outer_tuple(outer_tuple);
  }

  RC rc = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  return RC::SUCCESS;
}

RC ProjectPhysicalOperator::next()
{
  /// 由于我改了代码，导致之前的题目NULL过不了.....
  /// 我的源思路是为了处理 select length('das') 这个case
  /// 但是优化阶段会将NULL相关的题目后续算子清除
  if (no_child && !emitted_) {
    emitted_                = true;
    const auto &expressions = tuple_.expressions();
    for (const auto &expression : expressions) {
      if (expression->type() == ExprType::FIELD) {
        return RC::RECORD_EOF;
      } else if (expression->type() == ExprType::SYS_FUNCTION) {
        const auto &params = static_cast<SysFunctionExpr *>(expression.get())->params();
        for (const auto &param : params) {
          if (param->type() == ExprType::FIELD) {
            return RC::RECORD_EOF;
          }
        }
      }
    }
    return RC::SUCCESS;
  }
  if (children_.empty()) {
    return RC::RECORD_EOF;
  }
  return children_[0]->next();
}

RC ProjectPhysicalOperator::close()
{
  if (!children_.empty()) {
    children_[0]->close();
  }
  return RC::SUCCESS;
}
Tuple *ProjectPhysicalOperator::current_tuple()
{
  if (no_child) {
    return &tuple_;
  }
  tuple_.set_tuple(children_[0]->current_tuple());
  return &tuple_;
}

RC ProjectPhysicalOperator::tuple_schema(TupleSchema &schema) const
{
  // 当出现多表join时，可能会有多个表的字段需要输出
  for (const unique_ptr<Expression> &expression : expressions_) {
    std::string column_name;
    if (expression->type() == ExprType::FIELD) {
      // 如果是字段表达式，获取表名和字段名，拼接成 "table_name.field_name"
      FieldExpr  *field_expr = static_cast<FieldExpr *>(expression.get());
      std::string field_name = field_expr->field_name();
      // table name作为const char*存在，判断不为空指针且不为空字符串
      if (field_expr->try_get_table_name_in_multi_table_query() != nullptr &&
          strlen(field_expr->try_get_table_name_in_multi_table_query()) > 0) {
        std::string table_name = field_expr->try_get_table_name_in_multi_table_query();
        column_name            = table_name + "." + field_name;
      } else {
        column_name = field_name;  // 如果没有表名，仅使用字段名
      }
    } else {
      // 对于非字段表达式（例如计算表达式），使用 expression->name()
      column_name = expression->name();
    }
    if (!expression->alias().empty()) {
      column_name = expression->alias();
    }
    LOG_DEBUG("add column %s", column_name.c_str());
    schema.append_cell(column_name.c_str());
  }
  return RC::SUCCESS;
}