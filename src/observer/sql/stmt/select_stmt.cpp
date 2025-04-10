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
// Created by Wangyunlai on 2022/6/6.
//

#include "sql/stmt/select_stmt.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "sql/stmt/filter_stmt.h"
#include "storage/db/db.h"
#include "storage/table/table.h"
#include "sql/parser/expression_binder.h"

using namespace std;
using namespace common;

SelectStmt::~SelectStmt()
{
  if (nullptr != filter_stmt_) {
    delete filter_stmt_;
    filter_stmt_ = nullptr;
  }
}

RC SelectStmt::convert_alias_to_name(Expression *expr, std::shared_ptr<std::unordered_map<string, string>> alias2name)
{
  // 对表达式进行递归替换
  if (expr->type() == ExprType::VALUE) {
    return RC::SUCCESS;
  }
  if (expr->type() == ExprType::ARITHMETIC) {
    ArithmeticExpr *arith_expr = static_cast<ArithmeticExpr *>(expr);
    if (arith_expr->left() != nullptr) {
      RC rc = SelectStmt::convert_alias_to_name(arith_expr->left().get(), alias2name);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to check parent relation");
        return rc;
      }
    }
    if (arith_expr->right() != nullptr) {
      RC rc = SelectStmt::convert_alias_to_name(arith_expr->right().get(), alias2name);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to check parent relation");
        return rc;
      }
    }
    return RC::SUCCESS;
  } else if (expr->type() == ExprType::UNBOUND_AGGREGATION) {
    UnboundAggregateExpr *aggre_expr = static_cast<UnboundAggregateExpr *>(expr);
    if (aggre_expr->child() == nullptr) {
      LOG_WARN("invalid aggre expr");
      return RC::INVALID_ARGUMENT;
    }
    RC rc = SelectStmt::convert_alias_to_name(aggre_expr->child().get(), alias2name);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to check parent relation");
      return rc;
    }
    return rc;
  } else if (expr->type() == ExprType::SYS_FUNCTION) {
    SysFunctionExpr *sys_func_expr = static_cast<SysFunctionExpr *>(expr);
    if (sys_func_expr->params().size() == 0) {
      LOG_WARN("invalid sys function expr");
      return RC::INVALID_ARGUMENT;
    }
    for (auto &param : sys_func_expr->params()) {
      RC rc = SelectStmt::convert_alias_to_name(param.get(), alias2name);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to check parent relation");
        return rc;
      }
    }
    return RC::SUCCESS;
  } else if (expr->type() == ExprType::STAR) {
    const char *table_name = static_cast<StarExpr *>(expr)->table_name();
    if (!is_blank(table_name) && 0 != strcmp(table_name, "*")) {
      if (alias2name->find(table_name) != alias2name->end()) {
        // 如果在 alias2name 中找到了，说明是别名，需要替换为真实的表名
        std::string true_table_name = (*alias2name)[table_name];
        LOG_DEBUG("convert alias to name: %s -> %s", table_name, true_table_name.c_str());
        static_cast<StarExpr *>(expr)->set_table_name(true_table_name.c_str());
        expr->set_table_alias(table_name);
      }
    }
    // 如果没有表名，说明是 *，不需要替换
    return RC::SUCCESS;
  }
  if (expr->type() != ExprType::UNBOUND_FIELD) {
    LOG_WARN("convert_alias_to_name: invalid expr type: %d. It should be UnoundField.", expr->type());
    return RC::INVALID_ARGUMENT;
  }
  auto ub_field_expr = static_cast<UnboundFieldExpr *>(expr);

  // 替换 field 的表的别名为真实的表名
  if (alias2name->find(ub_field_expr->table_name()) != alias2name->end()) {
    // 如果在 alias2name 中找到了，说明是别名，需要替换为真实的表名
    std::string true_table_name = (*alias2name)[ub_field_expr->table_name()];
    LOG_DEBUG("convert alias to name: %s -> %s",ub_field_expr->table_name(), true_table_name.c_str());
    // THE LOGIC IS SO IMPORTANT HERE
    ub_field_expr->set_table_alias(ub_field_expr->table_name());
    ub_field_expr->set_table_name(true_table_name);
  }
  return RC::SUCCESS;
}

RC SelectStmt::create(Db *db, SelectSqlNode &select_sql, Stmt *&stmt,
    std::shared_ptr<std::unordered_map<string, string>> name2alias,
    std::shared_ptr<std::unordered_map<string, string>> alias2name)
{
  if (nullptr == db) {
    LOG_WARN("invalid argument. db is null");
    return RC::INVALID_ARGUMENT;
  }

  if (name2alias == nullptr)
    name2alias = std::make_shared<std::unordered_map<string, string>>();
  if (alias2name == nullptr)
    alias2name = std::make_shared<std::unordered_map<string, string>>();
  BinderContext binder_context;

  // collect tables in `from` statement
  vector<Table *>                tables;
  unordered_map<string, Table *> table_map;
  std::vector<std::string>       table_aliases;

  for (size_t i = 0; i < select_sql.relations.size(); i++) {
    const char *table_name = select_sql.relations[i].relation_name.c_str();
    if (nullptr == table_name) {
      LOG_WARN("invalid argument. relation name is null. index=%d", i);
      return RC::INVALID_ARGUMENT;
    }

    Table *table = db->find_table(table_name);
    if (nullptr == table) {
      LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
      return RC::SCHEMA_TABLE_NOT_EXIST;
    }

    binder_context.add_table(table);
    tables.push_back(table);
    // 当别名为空时也会加入“”
    table_aliases.push_back(select_sql.relations[i].alias_name);
    table_map.insert({table_name, table});
    // 检查 alias 重复
    for (size_t j = i + 1; j < select_sql.relations.size(); j++) {
      if (select_sql.relations[i].alias_name.empty() || select_sql.relations[j].alias_name.empty())
        continue;
      if (select_sql.relations[i].alias_name == select_sql.relations[j].alias_name) {
        LOG_WARN("duplicate alias: %s", select_sql.relations[i].alias_name.c_str());
        return RC::INVALID_ARGUMENT;
      }
    }

    if (!select_sql.relations[i].alias_name.empty()) {
      // 如果有别名，使用别名
      (*alias2name)[select_sql.relations[i].alias_name] = string(table_name);
      (*name2alias)[table_name]                         = select_sql.relations[i].alias_name;
    }
  }

  // 处理 select 中的表达式(projection)中带有别名的字段替换为真实表名,列名不会替换
  for (auto &expression : select_sql.expressions) {
    RC rc = convert_alias_to_name(expression.get(), alias2name);
    LOG_DEBUG("convert alias from %s to %s", expression->name(), expression->alias().c_str());
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to convert alias to name");
      return rc;
    }

    // 如果是 StarExpr，检查是否有别名，如果有报错
    if (expression->type() == ExprType::STAR) {
      StarExpr *star_expr = static_cast<StarExpr *>(expression.get());
      if (!star_expr->alias().empty()) {
        LOG_WARN("alias found in star expression");
        return RC::INVALID_ARGUMENT;
      }
    }
  }

  // 将 conditions 中 **所有** 带有别名的表名替换为真实的表名
  for (auto &condition : select_sql.conditions) {
    if (condition.left_expr != nullptr) {
      RC rc = convert_alias_to_name(condition.left_expr.get(), alias2name);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to convert alias to name");
        return rc;
      }
    }
    if (condition.right_expr != nullptr) {
      RC rc = convert_alias_to_name(condition.right_expr.get(), alias2name);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to convert alias to name");
        return rc;
      }
    }
  }

  // 绑定表达式，指的是将表达式中的字段和 table 关联起来
  //  collect query fields in `select` statement
  vector<unique_ptr<Expression>> bound_expressions;
  ExpressionBinder               expression_binder(binder_context);

  // 处理非聚合函数以及聚合函数, 保证非聚合函数对应的相关字段在group by中出现
  bool has_aggregation = false;
  for (unique_ptr<Expression> &expression : select_sql.expressions) {
    if (expression->type() == ExprType::UNBOUND_AGGREGATION) {
      has_aggregation = true;
      break;
    }
  }
  if (has_aggregation) {
    for (unique_ptr<Expression> &select_expr : select_sql.expressions) {
      if (select_expr->type() == ExprType::UNBOUND_AGGREGATION) {
        continue;
      }

      // 聚合函数的算术运算
      if (select_expr->type() == ExprType::ARITHMETIC) {
        ArithmeticExpr *arith_expr = static_cast<ArithmeticExpr *>(select_expr.get());
        if (arith_expr->left() != nullptr && arith_expr->left()->type() == ExprType::UNBOUND_AGGREGATION &&
            arith_expr->right() != nullptr && arith_expr->right()->type() == ExprType::UNBOUND_AGGREGATION) {
          continue;
        }
      }

      bool found = false;
      for (unique_ptr<Expression> &group_by_expr : select_sql.group_by) {
        if (select_expr->equal(*group_by_expr)) {
          found = true;
          break;
        }
      }
      if (!found) {
        LOG_WARN("non-aggregation expression found in select statement but not in group by statement");
        return RC::INVALID_ARGUMENT;
      }
    }
  }

  for (unique_ptr<Expression> &expression : select_sql.expressions) {
    RC rc = expression_binder.bind_expression(expression, bound_expressions);
    if (OB_FAIL(rc)) {
      LOG_INFO("bind expression failed. rc=%s", strrc(rc));
      return rc;
    }
  }

  // 处理 group by
  vector<unique_ptr<Expression>> group_by_expressions;
  for (unique_ptr<Expression> &expression : select_sql.group_by) {
    RC rc = expression_binder.bind_expression(expression, group_by_expressions);
    if (OB_FAIL(rc)) {
      LOG_INFO("bind expression failed. rc=%s", strrc(rc));
      return rc;
    }
  }

  Table *default_table = nullptr;
  if (tables.size() == 1) {
    default_table = tables[0];
  }

  // create filter statement in `where` statement
  FilterStmt *filter_stmt = nullptr;
  RC          rc          = FilterStmt::create(db, default_table, &table_map, select_sql.conditions, filter_stmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("cannot construct filter stmt");
    return rc;
  }

  // everything alright
  SelectStmt *select_stmt = new SelectStmt();

  select_stmt->tables_.swap(tables);
  select_stmt->table_aliases_.swap(table_aliases);
  select_stmt->query_expressions_.swap(bound_expressions);
  select_stmt->filter_stmt_ = filter_stmt;
  select_stmt->group_by_.swap(group_by_expressions);
  stmt = select_stmt;
  return RC::SUCCESS;
}
