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

#include "sql/stmt/filter_stmt.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "common/sys/rc.h"
#include "storage/db/db.h"
#include "storage/table/table.h"
#include "sql/parser/expression_binder.h"

FilterStmt::~FilterStmt()
{
  // 可以直接隐式管理内存
  // for (auto &condition : conditions_) {
  //   condition.reset();
  // }
  conditions_.clear();
}

/**
 * @brief 创建FilterStmt
 */
RC FilterStmt::create(Db *db, Table *default_table, std::unordered_map<std::string, Table *> *tables,
    vector<ConditionSqlNode> &conditions, FilterStmt *&stmt)
{
  // ConditionSqlNode --> ComparisionExpr --> ConjuctionExpr(默认都是AND) --> bindExpression
  stmt = nullptr;
  vector<unique_ptr<Expression>> bound_conditions;
  vector<unique_ptr<Expression>> conditions_exprs;
  // 绑定条件表达式, 将条件表达式中的字段解析为具体的表字段
  BinderContext binder_context;
  for (auto &table : *tables) {
    // context中同时考虑到了父查询中的Table
    binder_context.add_table(table.second);
  }
  ExpressionBinder expression_binder(binder_context);

  auto *tmp_stmt = new FilterStmt();
  // 重构条件表达式
  for (auto &condition : conditions) {
    switch (condition.comp_op) {
      case CompOp::EQUAL_TO:
      case CompOp::LESS_EQUAL:
      case CompOp::NOT_EQUAL:
      case CompOp::LESS_THAN:
      case CompOp::GREAT_EQUAL:
      case CompOp::GREAT_THAN:
      /// for sub-query
      case CompOp::IN_OP:
      case CompOp::NOT_IN_OP:
      case CompOp::EXISTS_OP:
      case CompOp::NOT_EXISTS_OP: {
        if (condition.comp_op == CompOp::IN_OP)
          LOG_DEBUG("Processing IN_OP condition");
        conditions_exprs.emplace_back(
            new ComparisonExpr(condition.comp_op, std::move(condition.left_expr), std::move(condition.right_expr)));
      } break;
      case CompOp::IS:
      case CompOp::IS_NOT: {
        conditions_exprs.emplace_back(
            new IsExpr(condition.comp_op, std::move(condition.left_expr), std::move(condition.right_expr)));
      } break;
      case CompOp::LIKE_OP:
      case CompOp::NOT_LIKE_OP: {
        conditions_exprs.emplace_back(
            new LikeExpr(condition.comp_op, std::move(condition.left_expr), std::move(condition.right_expr)));
      } break;
      default: {
        LOG_WARN("unsupported condition operator. comp_op=%d", condition.comp_op);
        return RC::INVALID_ARGUMENT;
      }
    }
  }

  for (auto &condition : conditions_exprs) {
    RC rc = expression_binder.bind_expression(condition, bound_conditions);
    if (rc != RC::SUCCESS) {
      delete tmp_stmt;
      LOG_WARN("failed to bind expression. rc=%s, condition=%s", strrc(rc), condition->name());
      return rc;
    }
  }
  // 各个表达式名称在语法分析阶段就被设置好了
  tmp_stmt->conditions_.swap(bound_conditions);

  stmt = tmp_stmt;
  return RC::SUCCESS;
}
