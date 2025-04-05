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
    std::vector<ConditionSqlNode> &conditions, FilterStmt *&stmt)
{
  // ConditionSqlNode --> ComparisionExpr --> ConjuctionExpr(默认都是AND) --> bindExpression
  stmt = nullptr;
  vector<unique_ptr<Expression>> bound_conditions;

  // 绑定条件表达式, 将条件表达式中的字段解析为具体的表字段
  BinderContext binder_context;
  for (auto &table : *tables) {
    binder_context.add_table(table.second);
  }
  ExpressionBinder expression_binder(binder_context);

  auto *tmp_stmt = new FilterStmt();
  for (auto &condition : conditions) {
    switch (condition.comp_op) {
      case CompOp::EQUAL_TO:
      case CompOp::LESS_EQUAL:
      case CompOp::NOT_EQUAL:
      case CompOp::LESS_THAN:
      case CompOp::GREAT_EQUAL:
      case CompOp::GREAT_THAN: {
        unique_ptr<Expression> condition_expr(
            new ComparisonExpr(condition.comp_op, std::move(condition.left_expr), std::move(condition.right_expr)));
        // 设置表达式名称
        string name = std::string(dynamic_cast<ComparisonExpr *>(condition_expr.get())->left()->name()) + " ";
        condition_expr->set_name(string(dynamic_cast<ComparisonExpr *>(condition_expr.get())->left()->name()) + " " +
                                 comp_op_to_string(condition.comp_op) + " " +
                                 dynamic_cast<ComparisonExpr *>(condition_expr.get())->right()->name());

        RC rc = expression_binder.bind_expression(condition_expr, bound_conditions);
        if (rc != RC::SUCCESS) {
          delete tmp_stmt;
          LOG_WARN("failed to bind expression. rc=%s, condition=%s", strrc(rc),
              condition_expr->name());
          return rc;
        }
      } break;
      default: {
        LOG_WARN("unsupported condition operator. comp_op=%d", condition.comp_op);
        return RC::INVALID_ARGUMENT;
      }
    }
  }

  tmp_stmt->conditions_.swap(bound_conditions);

  stmt = tmp_stmt;
  return RC::SUCCESS;
}
