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
// Created by Wangyunlai on 2022/07/05.
//

#include "sql/expr/expression.h"
#include "sql/expr/tuple.h"
#include "sql/expr/arithmetic_operator.hpp"
#include "common/lang/iomanip.h"
#include "common/lang/sstream.h"
#include "sql/parser/parse_defs.h"
#include "sql/operator/physical_operator.h"
#include "sql/operator/logical_operator.h"
#include "sql/stmt/select_stmt.h"
class SelectStmt;
class ParsedSqlNode;
class LogicalOperator;
class PhysicalOperator;

using namespace std;

std::string expr_type_to_string(ExprType type)
{
  switch (type) {
    case ExprType::NONE: return "NONE";
    case ExprType::STAR: return "STAR";
    case ExprType::UNBOUND_FIELD: return "UNBOUND_FIELD";
    case ExprType::UNBOUND_AGGREGATION: return "UNBOUND_AGGREGATION";
    case ExprType::FIELD: return "FIELD";
    case ExprType::VALUE: return "VALUE";
    case ExprType::CAST: return "CAST";
    case ExprType::COMPARISON: return "COMPARISON";
    case ExprType::CONJUNCTION: return "CONJUNCTION";
    case ExprType::ARITHMETIC: return "ARITHMETIC";
    case ExprType::AGGREGATION: return "AGGREGATION";
    case ExprType::LIKE: return "LIKE";
    case ExprType::IS: return "IS";
    case ExprType::SYS_FUNCTION: return "SYS_FUNCTION";
    case ExprType::VALUES: return "VALUES";
    case ExprType::SUB_QUERY: return "SUB_QUERY";
    default: return "UNKNOWN";
  }
}

RC FieldExpr::get_value(const Tuple &tuple, Value &value) const
{
  // change the table alias
  auto spec = TupleCellSpec(table_name(), field_name());
  if (!table_alias().empty())
    spec.set_table_alias(table_alias());
  return tuple.find_cell(spec, value);
}

bool FieldExpr::equal(const Expression &other) const
{
  if (this == &other) {
    return true;
  }
  if (other.type() != ExprType::FIELD) {
    return false;
  }
  const auto &other_field_expr = static_cast<const FieldExpr &>(other);
  return table_name() == other_field_expr.table_name() && field_name() == other_field_expr.field_name();
}

// TODO: 在进行表达式计算时，`chunk` 包含了所有列，因此可以通过 `field_id` 获取到对应列。
// 后续可以优化成在 `FieldExpr` 中存储 `chunk` 中某列的位置信息。
RC FieldExpr::get_column(Chunk &chunk, Column &column)
{
  if (pos_ != -1) {
    column.reference(chunk.column(pos_));
  } else {
    column.reference(chunk.column(field().meta()->field_id()));
  }
  return RC::SUCCESS;
}

bool ValueExpr::equal(const Expression &other) const
{
  if (this == &other) {
    return true;
  }
  if (other.type() != ExprType::VALUE) {
    return false;
  }
  const auto &other_value_expr = static_cast<const ValueExpr &>(other);
  return value_.compare(other_value_expr.get_value()) == 0;
}

RC ValueExpr::get_value(const Tuple &tuple, Value &value) const
{
  value = value_;
  return RC::SUCCESS;
}

RC ValueExpr::get_column(Chunk &chunk, Column &column)
{
  column.init(value_);
  return RC::SUCCESS;
}

/////////////////////////////////////////////////////////////////////////////////
CastExpr::CastExpr(unique_ptr<Expression> child, AttrType cast_type) : child_(std::move(child)), cast_type_(cast_type)
{}

CastExpr::~CastExpr() {}

RC CastExpr::cast(const Value &value, Value &cast_value) const
{
  RC rc = RC::SUCCESS;
  if (this->value_type() == value.attr_type()) {
    cast_value = value;
    return rc;
  }
  rc = Value::cast_to(value, cast_type_, cast_value);
  return rc;
}

RC CastExpr::get_value(const Tuple &tuple, Value &result) const
{
  Value value;
  RC    rc = child_->get_value(tuple, value);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  return cast(value, result);
}

RC CastExpr::try_get_value(Value &result) const
{
  Value value;
  RC    rc = child_->try_get_value(value);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  return cast(value, result);
}

////////////////////////////////////////////////////////////////////////////////

ComparisonExpr::ComparisonExpr(CompOp comp, unique_ptr<Expression> left, unique_ptr<Expression> right)
    : comp_(comp), left_(std::move(left)), right_(std::move(right))
{}

ComparisonExpr::~ComparisonExpr() {}

RC ComparisonExpr::compare_value(const Value &left, const Value &right, bool &result) const
{
  RC  rc         = RC::SUCCESS;
  int cmp_result = left.compare(right);
  result         = false;
  if (cmp_result == INT32_MAX) {
    // 处理如果有一个值为NULL的情况
    result = false;
    return rc;
  }
  switch (comp_) {
    case IN_OP:
    case EQUAL_TO: {
      result = (0 == cmp_result);
    } break;
    case LESS_EQUAL: {
      result = (cmp_result <= 0);
    } break;
    case NOT_IN_OP:
    case NOT_EQUAL: {
      result = (cmp_result != 0);
    } break;
    case LESS_THAN: {
      result = (cmp_result < 0);
    } break;
    case GREAT_EQUAL: {
      result = (cmp_result >= 0);
    } break;
    case GREAT_THAN: {
      result = (cmp_result > 0);
    } break;
    default: {
      LOG_WARN("unsupported comparison. %d", comp_);
      rc = RC::INTERNAL;
    } break;
  }

  return rc;
}

RC ComparisonExpr::try_get_value(Value &cell) const
{
  if (left_->type() == ExprType::VALUE && right_->type() == ExprType::VALUE) {
    ValueExpr   *left_value_expr  = static_cast<ValueExpr *>(left_.get());
    ValueExpr   *right_value_expr = static_cast<ValueExpr *>(right_.get());
    const Value &left_cell        = left_value_expr->get_value();
    const Value &right_cell       = right_value_expr->get_value();

    bool value = false;
    RC   rc    = compare_value(left_cell, right_cell, value);
    LOG_DEBUG("Through try_get_value(value): left_value=%s, right_value=%s", left_cell.to_string(), right_cell.to_string());
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to compare tuple cells. rc=%s", strrc(rc));
    } else {
      cell.set_boolean(value);
    }
    return rc;
  }

  return RC::INVALID_ARGUMENT;
}

// 需要进行大幅修改
RC ComparisonExpr::get_value(const Tuple &tuple, Value &value) const
{
  Value left_value;
  Value right_value;
  RC    rc = RC::SUCCESS;
  // exists or not_exists
  if (left_ == nullptr && right_->type() == ExprType::SUB_QUERY) {
    SubqueryExpr *subquery_expr;
    Value        *sub_query_value;
    subquery_expr   = static_cast<SubqueryExpr *>(right_.get());
    sub_query_value = &right_value;
    bool bool_value = false;
    // 循环执行子查询的算子，直到找到一个满足条件的值
    while ((rc = subquery_expr->get_value(tuple, *sub_query_value)) == RC::SUCCESS) {
      // fast-break
      if (comp_ == CompOp::EXISTS_OP) {
        bool_value = true;
        value.set_boolean(true);
        break;
      } else if (comp_ == CompOp::NOT_EXISTS_OP) {
        bool_value = false;
        value.set_boolean(false);
        break;
      }
      // CompOp == EXISTS/NOT_EXISTS 不可能走到这里
      ASSERT(false, "won't be here");
    }
    // 执行到了算子末尾，还没有找到满足条件的值
    if (rc == RC::RECORD_EOF) {
      if (comp_ == CompOp::NOT_EXISTS_OP) {
        bool_value = true;
        rc         = RC::SUCCESS;
      } else if (comp_ == CompOp::EXISTS_OP) {
        bool_value = false;
        rc         = RC::SUCCESS;
      }
    }
    if (rc != RC::SUCCESS && rc != RC::RECORD_EOF) {
      LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
    }
    value.set_boolean(bool_value);

    // 关闭算子
    if (subquery_expr->physical_operator() != nullptr) {
      if (subquery_expr->close_physical_operator() != RC::SUCCESS)
        LOG_WARN("failed to close physical operator.");
    }
  }
  // 子查询 comp_op 子查询
  else if (left_->type() == ExprType::SUB_QUERY && right_->type() == ExprType::SUB_QUERY) {
    // 获取子查询的值
    rc = left_->get_value(tuple, left_value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
      return rc;
    }
    // 左子查询的结果目前只能有一行
    Value _test;
    if (left_->get_value(tuple, _test) != RC::RECORD_EOF) {
      LOG_WARN("we only support 1 rows for subquery result rc=%s", strrc(rc));
      return RC::INTERNAL;
    }
    rc = right_->get_value(tuple, right_value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
      return rc;
    }
    if (right_->get_value(tuple, _test) != RC::RECORD_EOF) {
      LOG_WARN("we only support 1 rows for subquery result rc=%s", strrc(rc));
      return RC::INTERNAL;
    }
    bool bool_value = false;
    rc              = compare_value(left_value, right_value, bool_value);
    if (rc == RC::SUCCESS) {
      value.set_boolean(bool_value);
    }
  }
  // value in 子查询 | 子查询 comp_op value | value comp_op 子查询
  else if (left_->type() == ExprType::SUB_QUERY || right_->type() == ExprType::SUB_QUERY) {
    SubqueryExpr *subquery_expr;
    Value        *sub_query_value;
    if (left_->type() == ExprType::SUB_QUERY) {
      subquery_expr   = static_cast<SubqueryExpr *>(left_.get());
      rc              = right_->get_value(tuple, right_value);
      sub_query_value = &left_value;
    } else {
      subquery_expr   = static_cast<SubqueryExpr *>(right_.get());
      rc              = left_->get_value(tuple, left_value);
      sub_query_value = &right_value;
    }
    if (rc != RC::SUCCESS) {
      LOG_WARN("ComparisonExpr:Subquery: failed to get value of expression. rc=%s", strrc(rc));
      return rc;
    }

    bool bool_value = false;
    // 循环执行子查询的算子，直到找到一个满足条件的值
    bool has_sub_queried_ = false;
    while ((rc = subquery_expr->get_value(tuple, *sub_query_value)) == RC::SUCCESS) {
      if (comp_ != CompOp::IN_OP && comp_ != CompOp::NOT_IN_OP) {
        // 当 comp_ 不是 IN、NOT_IN时，子查询的结果只能是一个值
        if (has_sub_queried_) {
          has_sub_queried_ = false;
          LOG_WARN("ERROR: Subquery returns more than 1 row");
          rc = RC::INVALID_ARGUMENT;
          break;
        } else {
          has_sub_queried_ = true;
        }
      }

      rc = compare_value(left_value, right_value, bool_value);

      // 对 IN/NOT_IN 的 fast-break 逻辑
      // 当 CompOp 是 IN 时，只要找到一个满足条件的值就可以返回
      // 当 CompOp 是 NOT_IN 时，必须得遍历完所有的值才能知道
      if (rc == RC::SUCCESS && comp_ == CompOp::IN_OP && bool_value) {
        value.set_boolean(bool_value);
        break;
      }
      //// IMPORTANT:: 逻辑问题
      else if (rc == RC::SUCCESS && comp_ == CompOp::NOT_IN_OP && !bool_value) {
        // 当 CompOp==NOT_IN，且左右 value 相等（因为是NOTIN，所以bool_value前要加!），直接结束循环
        value.set_boolean(bool_value);
        break;
      }
    }

    // 执行到了算子末尾，还没有找到满足条件的值
    if (rc == RC::RECORD_EOF) {
      if (comp_ == CompOp::NOT_IN_OP) {
        bool_value = true;
        rc         = RC::SUCCESS;
      } else if (comp_ == CompOp::IN_OP) {
        bool_value = false;
        rc         = RC::SUCCESS;
      }
    }
    if (rc != RC::SUCCESS && rc != RC::RECORD_EOF) {
      LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
    }
    value.set_boolean(bool_value);

    // 关闭算子
    if (subquery_expr->physical_operator() != nullptr) {
      if (subquery_expr->close_physical_operator() != RC::SUCCESS)
        LOG_WARN("failed to close physical operator.");
    }
  }
  // value in value_list
  else if (left_->type() == ExprType::VALUES || right_->type() == ExprType::VALUES) {
    ValueListExpr *value_list_expr;
    Value         *value_list_value;
    if (left_->type() == ExprType::VALUES) {
      value_list_expr  = static_cast<ValueListExpr *>(left_.get());
      rc               = right_->get_value(tuple, right_value);  // 假设右边不是value list
      value_list_value = &left_value;
    } else {
      value_list_expr  = static_cast<ValueListExpr *>(right_.get());
      rc               = left_->get_value(tuple, left_value);  // 假设左边不是value list
      value_list_value = &right_value;
    }
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
      return rc;
    }

    bool bool_value       = false;
    bool has_sub_queried_ = false;

    // 循环执行子查询的算子，直到找到一个满足条件的值
    while ((rc = value_list_expr->get_value(tuple, *value_list_value)) == RC::SUCCESS) {
      if (value_list_value->attr_type() == AttrType::UNDEFINED) {
        rc = RC::RECORD_EOF;
        break;
      }

      if (comp_ == CompOp::EXISTS_OP) {
        // 当comp_为EXISTS时，直接返回true
        bool_value = true;
        value.set_boolean(true);
        break;
      } else if (comp_ == CompOp::NOT_EXISTS_OP) {
        bool_value = false;
        value.set_boolean(false);
        break;
      } else if (comp_ == CompOp::EQUAL_TO || comp_ == CompOp::NOT_EQUAL) {
        // 当comp_为=或者<>时，子查询的结果只能是一个值
        if (has_sub_queried_) {
          has_sub_queried_ = false;
          rc               = RC::INVALID_ARGUMENT;
          break;
        } else {
          has_sub_queried_ = true;
        }
      }

      rc = compare_value(left_value, right_value, bool_value);

      // 当CompOp不是NOT_IN时，只要找到一个满足条件的值就可以返回
      // 当CompOp是NOT_IN时，必须得遍历完所有的值才能知道
      if (rc == RC::SUCCESS && comp_ != CompOp::NOT_IN_OP && bool_value) {
        value.set_boolean(bool_value);
        break;
      } else if (rc == RC::SUCCESS && comp_ == CompOp::NOT_IN_OP && !bool_value) {
        // 当CompOp==NOT_IN，且左右value相等（因为是NOTIN，所以bool_value前要加!），直接结束循环
        value.set_boolean(bool_value);
        break;
      }
    }

    // EOF判断
    if (rc == RC::RECORD_EOF) {
      if (comp_ == CompOp::NOT_IN_OP || comp_ == CompOp::NOT_EXISTS_OP) {
        value.set_boolean(true);
        rc = RC::SUCCESS;
        return rc;
      } else if (comp_ == CompOp::IN_OP || comp_ == CompOp::EXISTS_OP) {
        value.set_boolean(false);
        rc = RC::SUCCESS;
        return rc;
      }
    }
    if (rc != RC::SUCCESS && rc != RC::RECORD_EOF) {
      LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
    }

    value.set_boolean(bool_value);

    value_list_expr->set_index(0);  // 重置index

  } else {

    if (comp_ == CompOp::EXISTS_OP || comp_ == CompOp::NOT_EXISTS_OP) {
      LOG_WARN("exists and not exists should be used in subquery");
      return RC::INVALID_ARGUMENT;
    }
    rc = left_->get_value(tuple, left_value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
      return rc;
    }
    rc = right_->get_value(tuple, right_value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
      return rc;
    }

    bool bool_value = false;
    rc              = compare_value(left_value, right_value, bool_value);
    if (rc == RC::SUCCESS) {
      value.set_boolean(bool_value);
    }
  }
  if (rc == RC::RECORD_EOF)
    rc = RC::SUCCESS;
  return rc;
}

RC ComparisonExpr::eval(Chunk &chunk, vector<uint8_t> &select)
{
  RC     rc = RC::SUCCESS;
  Column left_column;
  Column right_column;

  rc = left_->get_column(chunk, left_column);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
    return rc;
  }
  rc = right_->get_column(chunk, right_column);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
    return rc;
  }
  if (left_column.attr_type() != right_column.attr_type()) {
    LOG_WARN("cannot compare columns with different types");
    return RC::INTERNAL;
  }
  if (left_column.attr_type() == AttrType::INTS) {
    rc = compare_column<int>(left_column, right_column, select);
  } else if (left_column.attr_type() == AttrType::FLOATS) {
    rc = compare_column<float>(left_column, right_column, select);
  } else {
    // TODO: support string compare
    LOG_WARN("unsupported data type %d", left_column.attr_type());
    return RC::INTERNAL;
  }
  return rc;
}

template <typename T>
RC ComparisonExpr::compare_column(const Column &left, const Column &right, vector<uint8_t> &result) const
{
  RC rc = RC::SUCCESS;

  bool left_const  = left.column_type() == Column::Type::CONSTANT_COLUMN;
  bool right_const = right.column_type() == Column::Type::CONSTANT_COLUMN;
  if (left_const && right_const) {
    compare_result<T, true, true>((T *)left.data(), (T *)right.data(), left.count(), result, comp_);
  } else if (left_const && !right_const) {
    compare_result<T, true, false>((T *)left.data(), (T *)right.data(), right.count(), result, comp_);
  } else if (!left_const && right_const) {
    compare_result<T, false, true>((T *)left.data(), (T *)right.data(), left.count(), result, comp_);
  } else {
    compare_result<T, false, false>((T *)left.data(), (T *)right.data(), left.count(), result, comp_);
  }
  return rc;
}

////////////////////////////////////////////////////////////////////////////////
ConjunctionExpr::ConjunctionExpr(Type type, vector<unique_ptr<Expression>> &children)
    : conjunction_type_(type), children_(std::move(children))
{}

RC ConjunctionExpr::get_value(const Tuple &tuple, Value &value) const
{
  RC rc = RC::SUCCESS;
  if (children_.empty()) {
    value.set_boolean(true);
    return rc;
  }

  Value tmp_value;
  for (const unique_ptr<Expression> &expr : children_) {
    rc = expr->get_value(tuple, tmp_value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value by child expression. rc=%s", strrc(rc));
      return rc;
    }
    bool bool_value = tmp_value.get_boolean();
    if ((conjunction_type_ == Type::AND && !bool_value) || (conjunction_type_ == Type::OR && bool_value)) {
      value.set_boolean(bool_value);
      return rc;
    }
  }

  bool default_value = (conjunction_type_ == Type::AND);
  value.set_boolean(default_value);
  return rc;
}

////////////////////////////////////////////////////////////////////////////////

ArithmeticExpr::ArithmeticExpr(ArithmeticExpr::Type type, Expression *left, Expression *right)
    : arithmetic_type_(type), left_(left), right_(right)
{}
ArithmeticExpr::ArithmeticExpr(ArithmeticExpr::Type type, unique_ptr<Expression> left, unique_ptr<Expression> right)
    : arithmetic_type_(type), left_(std::move(left)), right_(std::move(right))
{}

bool ArithmeticExpr::equal(const Expression &other) const
{
  if (this == &other) {
    return true;
  }
  if (type() != other.type()) {
    return false;
  }
  auto &other_arith_expr = static_cast<const ArithmeticExpr &>(other);
  return arithmetic_type_ == other_arith_expr.arithmetic_type() && left_->equal(*other_arith_expr.left_) &&
         right_->equal(*other_arith_expr.right_);
}
AttrType ArithmeticExpr::value_type() const
{
  if (!right_) {
    return left_->value_type();
  }

  // 自动会转换为浮点数
  if (left_->value_type() == AttrType::INTS && right_->value_type() == AttrType::INTS &&
      arithmetic_type_ != Type::DIV) {
    return AttrType::INTS;
  }

  return AttrType::FLOATS;
}

RC ArithmeticExpr::calc_value(const Value &left_value, const Value &right_value, Value &value) const
{
  RC rc = RC::SUCCESS;

  const AttrType target_type = value_type();
  value.set_type(target_type);

  switch (arithmetic_type_) {
    case Type::ADD: {
      Value::add(left_value, right_value, value);
    } break;

    case Type::SUB: {
      Value::subtract(left_value, right_value, value);
    } break;

    case Type::MUL: {
      Value::multiply(left_value, right_value, value);
    } break;

    case Type::DIV: {
      Value::divide(left_value, right_value, value);
    } break;

    case Type::NEGATIVE: {
      Value::negative(left_value, value);
    } break;

    default: {
      rc = RC::INTERNAL;
      LOG_WARN("unsupported arithmetic type. %d", arithmetic_type_);
    } break;
  }
  return rc;
}

template <bool LEFT_CONSTANT, bool RIGHT_CONSTANT>
RC ArithmeticExpr::execute_calc(
    const Column &left, const Column &right, Column &result, Type type, AttrType attr_type) const
{
  RC rc = RC::SUCCESS;
  switch (type) {
    case Type::ADD: {
      if (attr_type == AttrType::INTS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, int, AddOperator>(
            (int *)left.data(), (int *)right.data(), (int *)result.data(), result.capacity());
      } else if (attr_type == AttrType::FLOATS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, float, AddOperator>(
            (float *)left.data(), (float *)right.data(), (float *)result.data(), result.capacity());
      } else {
        rc = RC::UNIMPLEMENTED;
      }
    } break;
    case Type::SUB:
      if (attr_type == AttrType::INTS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, int, SubtractOperator>(
            (int *)left.data(), (int *)right.data(), (int *)result.data(), result.capacity());
      } else if (attr_type == AttrType::FLOATS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, float, SubtractOperator>(
            (float *)left.data(), (float *)right.data(), (float *)result.data(), result.capacity());
      } else {
        rc = RC::UNIMPLEMENTED;
      }
      break;
    case Type::MUL:
      if (attr_type == AttrType::INTS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, int, MultiplyOperator>(
            (int *)left.data(), (int *)right.data(), (int *)result.data(), result.capacity());
      } else if (attr_type == AttrType::FLOATS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, float, MultiplyOperator>(
            (float *)left.data(), (float *)right.data(), (float *)result.data(), result.capacity());
      } else {
        rc = RC::UNIMPLEMENTED;
      }
      break;
    case Type::DIV:
      if (attr_type == AttrType::INTS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, int, DivideOperator>(
            (int *)left.data(), (int *)right.data(), (int *)result.data(), result.capacity());
      } else if (attr_type == AttrType::FLOATS) {
        binary_operator<LEFT_CONSTANT, RIGHT_CONSTANT, float, DivideOperator>(
            (float *)left.data(), (float *)right.data(), (float *)result.data(), result.capacity());
      } else {
        rc = RC::UNIMPLEMENTED;
      }
      break;
    case Type::NEGATIVE:
      if (attr_type == AttrType::INTS) {
        unary_operator<LEFT_CONSTANT, int, NegateOperator>((int *)left.data(), (int *)result.data(), result.capacity());
      } else if (attr_type == AttrType::FLOATS) {
        unary_operator<LEFT_CONSTANT, float, NegateOperator>(
            (float *)left.data(), (float *)result.data(), result.capacity());
      } else {
        rc = RC::UNIMPLEMENTED;
      }
      break;
    default: rc = RC::UNIMPLEMENTED; break;
  }
  if (rc == RC::SUCCESS) {
    result.set_count(result.capacity());
  }
  return rc;
}

RC ArithmeticExpr::get_value(const Tuple &tuple, Value &value) const
{
  RC rc = RC::SUCCESS;

  Value left_value;
  Value right_value;
  if (left_) {
    rc = left_->get_value(tuple, left_value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
      return rc;
    }
  }
  if (right_) {
    rc = right_->get_value(tuple, right_value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
      return rc;
    }
  }
  return calc_value(left_value, right_value, value);
}

RC ArithmeticExpr::get_column(Chunk &chunk, Column &column)
{
  RC rc = RC::SUCCESS;
  if (pos_ != -1) {
    column.reference(chunk.column(pos_));
    return rc;
  }
  Column left_column;
  Column right_column;

  rc = left_->get_column(chunk, left_column);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get column of left expression. rc=%s", strrc(rc));
    return rc;
  }
  rc = right_->get_column(chunk, right_column);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get column of right expression. rc=%s", strrc(rc));
    return rc;
  }
  return calc_column(left_column, right_column, column);
}

RC ArithmeticExpr::calc_column(const Column &left_column, const Column &right_column, Column &column) const
{
  RC rc = RC::SUCCESS;

  const AttrType target_type = value_type();
  column.init(target_type, left_column.attr_len(), max(left_column.count(), right_column.count()));
  bool left_const  = left_column.column_type() == Column::Type::CONSTANT_COLUMN;
  bool right_const = right_column.column_type() == Column::Type::CONSTANT_COLUMN;
  if (left_const && right_const) {
    column.set_column_type(Column::Type::CONSTANT_COLUMN);
    rc = execute_calc<true, true>(left_column, right_column, column, arithmetic_type_, target_type);
  } else if (left_const && !right_const) {
    column.set_column_type(Column::Type::NORMAL_COLUMN);
    rc = execute_calc<true, false>(left_column, right_column, column, arithmetic_type_, target_type);
  } else if (!left_const && right_const) {
    column.set_column_type(Column::Type::NORMAL_COLUMN);
    rc = execute_calc<false, true>(left_column, right_column, column, arithmetic_type_, target_type);
  } else {
    column.set_column_type(Column::Type::NORMAL_COLUMN);
    rc = execute_calc<false, false>(left_column, right_column, column, arithmetic_type_, target_type);
  }
  return rc;
}

RC ArithmeticExpr::try_get_value(Value &value) const
{
  RC rc = RC::SUCCESS;

  Value left_value;
  Value right_value;

  rc = left_->try_get_value(left_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
    return rc;
  }

  if (right_) {
    rc = right_->try_get_value(right_value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
      return rc;
    }
  }

  return calc_value(left_value, right_value, value);
}

////////////////////////////////////////////////////////////////////////////////

UnboundAggregateExpr::UnboundAggregateExpr(const char *aggregate_name, Expression *child)
    : aggregate_name_(aggregate_name), child_(child)
{}

////////////////////////////////////////////////////////////////////////////////
AggregateExpr::AggregateExpr(Type type, Expression *child) : aggregate_type_(type), child_(child) {}

AggregateExpr::AggregateExpr(Type type, unique_ptr<Expression> child) : aggregate_type_(type), child_(std::move(child))
{}

RC AggregateExpr::get_column(Chunk &chunk, Column &column)
{
  RC rc = RC::SUCCESS;
  if (pos_ != -1) {
    column.reference(chunk.column(pos_));
  } else {
    rc = RC::INTERNAL;
  }
  return rc;
}

bool AggregateExpr::equal(const Expression &other) const
{
  if (this == &other) {
    return true;
  }
  if (other.type() != type()) {
    return false;
  }
  const AggregateExpr &other_aggr_expr = static_cast<const AggregateExpr &>(other);
  return aggregate_type_ == other_aggr_expr.aggregate_type() && child_->equal(*other_aggr_expr.child());
}

unique_ptr<Aggregator> AggregateExpr::create_aggregator() const
{
  unique_ptr<Aggregator> aggregator;
  switch (aggregate_type_) {
    case Type::SUM: {
      aggregator = make_unique<SumAggregator>();
      break;
    }
    case Type::AVG: {
      aggregator = make_unique<AvgAggregator>();
      break;
    }
    case Type::COUNT: {
      aggregator = make_unique<CountAggregator>();
      break;
    }
    case Type::MAX: {
      aggregator = make_unique<MaxAggregator>();
      break;
    }
    case Type::MIN: {
      aggregator = make_unique<MinAggregator>();
      break;
    }
    default: {
      ASSERT(false, "unsupported aggregate type");
      break;
    }
  }
  return aggregator;
}

RC AggregateExpr::get_value(const Tuple &tuple, Value &value) const
{
  return tuple.find_cell(TupleCellSpec(name()), value);
}

RC AggregateExpr::type_from_string(const char *type_str, AggregateExpr::Type &type)
{
  RC rc = RC::SUCCESS;
  if (0 == strcasecmp(type_str, "count")) {
    type = Type::COUNT;
  } else if (0 == strcasecmp(type_str, "sum")) {
    type = Type::SUM;
  } else if (0 == strcasecmp(type_str, "avg")) {
    type = Type::AVG;
  } else if (0 == strcasecmp(type_str, "max")) {
    type = Type::MAX;
  } else if (0 == strcasecmp(type_str, "min")) {
    type = Type::MIN;
  } else {
    rc = RC::INVALID_ARGUMENT;
  }
  return rc;
}

////////////////////////////////////////////////////////////////////////////////
AttrType SysFunctionExpr::value_type() const
{
  switch (sys_func_type_) {
    case SysFuncType::DATE_FORMAT: return AttrType::DATES;
    case SysFuncType::LENGTH: return AttrType::CHARS;
    case SysFuncType::ROUND: return AttrType::FLOATS;
    default: return AttrType::UNDEFINED;
  }
  return AttrType::UNDEFINED;
}

RC SysFunctionExpr::check_params_type_and_number() const
{
  switch (sys_func_type_) {
    case SysFuncType::DATE_FORMAT: {
      if (params_.size() != 2 || params_[0]->value_type() != AttrType::DATES ||
          params_[1]->value_type() != AttrType::CHARS) {
        LOG_WARN("DATE_FORMAT function must have two parameters, the first is date and the second is chars");
        return RC::INVALID_ARGUMENT;
      }
      /// TODO: check the type of params
      break;
    }
    case SysFuncType::LENGTH: {
      if (params_.size() != 1 && params_[0]->value_type() != this->value_type()) {
        LOG_WARN("LENGTH function must have one parameter, which is chars type");
        return RC::INVALID_ARGUMENT;
      }
      break;
    }
    case SysFuncType::ROUND: {
      /// TODO: check the type of params
      if ((params_.size() == 1 || params_.size() != 2) && params_[0]->value_type() != AttrType::FLOATS) {
        LOG_WARN("ROUND function must have one or two parameters, the first is float and the second is int");
        return RC::INVALID_ARGUMENT;
      }
      if (params_.size() == 2 && params_[1]->value_type() != AttrType::INTS) {
        LOG_WARN("ROUND function's second parameter must be int");
        return RC::INVALID_ARGUMENT;
      }
      break;
    }
    default: {
      LOG_WARN("unsupported sys function type. %d", sys_func_type_);
      return RC::UNIMPLEMENTED;
    }
  }
  return RC::SUCCESS;
}

RC SysFunctionExpr::get_func_length_value(const Tuple &tuple, Value &value) const
{
  RC    rc = RC::SUCCESS;
  Value param;
  rc = params_[0]->get_value(tuple, param);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of first parameter. rc=%s", strrc(rc));
    return rc;
  }
  if (param.attr_type() != AttrType::CHARS) {
    LOG_WARN("LENGTH function's parameter must be CHAR");
    return RC::INVALID_ARGUMENT;
  }
  int len = strlen(param.get_string().c_str());
  value.set_int(len);
  return rc;
}

RC SysFunctionExpr::get_func_round_value(const Tuple &tuple, Value &value) const
{
  RC rc = RC::SUCCESS;
  if (params_.size() > 1) {
    Value param1;
    Value param2;
    rc = params_[0]->get_value(tuple, param1);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value of first parameter. rc=%s", strrc(rc));
      return rc;
    }
    rc = params_[1]->get_value(tuple, param2);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value of second parameter. rc=%s", strrc(rc));
      return rc;
    }
    float        value1 = param1.get_float();
    int          value2 = param2.get_int();
    stringstream ss;
    ss << fixed << setprecision(value2) << value1;
    ss >> value1;
    value.set_float(value1);
  } else {
    Value param1;
    rc = params_[0]->get_value(tuple, param1);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value of first parameter. rc=%s", strrc(rc));
      return rc;
    }
    float        value1 = param1.get_float();
    stringstream ss;
    ss << fixed << setprecision(0) << value1;
    ss >> value1;
    value.set_float(value1);
  }
  return rc;
}

RC SysFunctionExpr::get_func_date_format_value(const Tuple &tuple, Value &value) const
{
  RC rc = RC::SUCCESS;
  /// input: select date_format('2001-02-04', '%y/%m/%d') as date_type;
  /// ans:- 01/02/04
  Value param1;
  Value param2;
  // 获取第一个参数（日期）

  rc = params_[0]->get_value(tuple, param1);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of first parameter. rc=%s", strrc(rc));
    return rc;
  }
  rc = params_[1]->get_value(tuple, param2);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of second parameter. rc=%s", strrc(rc));
    return rc;
  }

  // 获取日期和格式字符串
  int         date_val   = param1.get_int();
  const char *format_str = param2.data();

  // 分解日期
  int year  = date_val / 10000;
  int month = (date_val % 10000) / 100;
  int day   = date_val % 100;

  // 转换格式
  std::string result;
  for (size_t i = 0; format_str[i] != '\0'; ++i) {
    if (format_str[i] == '%' && format_str[i + 1] != '\0') {
      switch (format_str[++i]) {
        case 'y': {  // 两位数的年份
          result += std::to_string(year % 100 / 10) + std::to_string(year % 10);
          break;
        }
        case 'Y': {  // 四位数的年份
          result += std::to_string(year);
          break;
        }
        case 'm': {  // 两位数的月份（01-12）
          if (month < 10) {
            result += "0";
          }
          result += std::to_string(month);
          break;
        }
        case 'M': {  // 月份名称（January-December）
          static const char *month_names[] = {"January",
              "February",
              "March",
              "April",
              "May",
              "June",
              "July",
              "August",
              "September",
              "October",
              "November",
              "December"};
          if (month >= 1 && month <= 12) {
            result += month_names[month - 1];
          } else {
            return RC::INVALID_ARGUMENT;  // 无效的月份
          }
          break;
        }
        // 先不考虑valid...
        case 'd': {  // 两位数的日期（01-31）
          if (day < 10) {
            result += "0";
          }
          result += std::to_string(day);
          break;
        }
        case 'D': {  // 有英文后缀的日期（1st, 2nd, 3rd, ...）
          result += std::to_string(day);
          if (day >= 11 && day <= 13) {
            result += "th";
          } else {
            switch (day % 10) {
              case 1: result += "st"; break;
              case 2: result += "nd"; break;
              case 3: result += "rd"; break;
              default: result += "th"; break;
            }
          }
          break;
        }
        case '%': {  // 百分号字符
          result += "%";
          break;
        }
        default: {  // 不支持的格式说明符
          result += std::string(1, format_str[i]);
          break;
        }
      }
    } else {
      result += format_str[i];
    }
  }

  // 设置返回值为格式化后的字符串
  value.set_type(AttrType::CHARS);
  value.set_data(result.c_str(), result.length());
  return rc;
}

RC SysFunctionExpr::try_get_func_length_value(Value &value) const
{
  RC    rc = RC::SUCCESS;
  Value param;
  rc = params_[0]->try_get_value(param);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of first parameter. rc=%s", strrc(rc));
    return rc;
  }
  if (param.attr_type() != AttrType::CHARS) {
    LOG_WARN("LENGTH function's parameter must be CHAR");
    return RC::INVALID_ARGUMENT;
  }
  int len = strlen(param.get_string().c_str());
  value.set_int(len);
  return rc;
}

RC SysFunctionExpr::try_get_func_round_value(Value &value) const
{
  RC rc = RC::SUCCESS;
  if (params_.size() > 1) {
    Value param1;
    Value param2;
    rc = params_[0]->try_get_value(param1);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value of first parameter. rc=%s", strrc(rc));
      return rc;
    }
    rc = params_[1]->try_get_value(param2);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value of second parameter. rc=%s", strrc(rc));
      return rc;
    }
    float        value1 = param1.get_float();
    int          value2 = param2.get_int();
    stringstream ss;
    ss << fixed << setprecision(value2) << value1;
    ss >> value1;
    value.set_float(value1);
  } else {
    Value param1;
    rc = params_[0]->try_get_value(param1);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get value of first parameter. rc=%s", strrc(rc));
      return rc;
    }
    float        value1 = param1.get_float();
    stringstream ss;
    ss << fixed << setprecision(0) << value1;
    ss >> value1;
    value.set_float(value1);
  }
  return rc;
}

RC SysFunctionExpr::try_get_func_date_format_value(Value &value) const
{
  RC rc = RC::SUCCESS;
  /// input: select date_format('2001-02-04', '%y/%m/%d') as date_type;
  /// ans:- 01/02/04
  Value param1;
  Value param2;
  // 获取第一个参数（日期）

  rc = params_[0]->try_get_value(param1);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of first parameter. rc=%s", strrc(rc));
    return rc;
  }
  rc = params_[1]->try_get_value(param2);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of second parameter. rc=%s", strrc(rc));
    return rc;
  }

  // 获取日期和格式字符串
  int         date_val   = param1.get_int();
  const char *format_str = param2.data();

  // 分解日期
  int year  = date_val / 10000;
  int month = (date_val % 10000) / 100;
  int day   = date_val % 100;

  // 转换格式
  std::string result;
  for (size_t i = 0; format_str[i] != '\0'; ++i) {
    if (format_str[i] == '%' && format_str[i + 1] != '\0') {
      switch (format_str[++i]) {
        case 'y': {  // 两位数的年份
          result += std::to_string(year % 100 / 10) + std::to_string(year % 10);
          break;
        }
        case 'Y': {  // 四位数的年份
          result += std::to_string(year);
          break;
        }
        case 'm': {  // 两位数的月份（01-12）
          if (month < 10) {
            result += "0";
          }
          result += std::to_string(month);
          break;
        }
        case 'M': {  // 月份名称（January-December）
          static const char *month_names[] = {"January",
              "February",
              "March",
              "April",
              "May",
              "June",
              "July",
              "August",
              "September",
              "October",
              "November",
              "December"};
          if (month >= 1 && month <= 12) {
            result += month_names[month - 1];
          } else {
            return RC::INVALID_ARGUMENT;  // 无效的月份
          }
          break;
        }
        // 先不考虑valid...
        case 'd': {  // 两位数的日期（01-31）
          if (day < 10) {
            result += "0";
          }
          result += std::to_string(day);
          break;
        }
        case 'D': {  // 有英文后缀的日期（1st, 2nd, 3rd, ...）
          result += std::to_string(day);
          if (day >= 11 && day <= 13) {
            result += "th";
          } else {
            switch (day % 10) {
              case 1: result += "st"; break;
              case 2: result += "nd"; break;
              case 3: result += "rd"; break;
              default: result += "th"; break;
            }
          }
          break;
        }
        case '%': {  // 百分号字符
          result += "%";
          break;
        }
        default: {  // 不支持的格式说明符
          result += std::string(1, format_str[i]);
          break;
        }
      }
    } else {
      result += format_str[i];
    }
  }

  // 设置返回值为格式化后的字符串
  value.set_type(AttrType::CHARS);
  LOG_DEBUG("HERE IS ! result: %s", result.c_str());
  value.set_data(result.c_str(), result.length());

  return rc;
}
////////////////////////////////////////////////////////////////////////////////
IsExpr::IsExpr(CompOp comp_op, std::unique_ptr<Expression> left, std::unique_ptr<Expression> right)
    : comp_(comp_op), left_(std::move(left)), right_(std::move(right))
{}

RC IsExpr::get_value(const Tuple &tuple, Value &value) const
{
  RC rc = RC::SUCCESS;
  if (comp_ != CompOp::IS && comp_ != CompOp::IS_NOT) {
    LOG_WARN("unsupported IS expression. %d", comp_);
    return RC::INTERNAL;
  }
  if (right_->type() != ExprType::VALUE) {
    LOG_WARN("right expression of IS must be a constant");
    return RC::INVALID_ARGUMENT;
  }
  Value left_value;
  Value right_value;
  rc = left_->get_value(tuple, left_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
    return rc;
  }
  rc = right_->get_value(tuple, right_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
    return rc;
  }
  if (right_value.is_null()) {
    if (comp_ == CompOp::IS) {
      value.set_boolean(left_value.is_null());
    } else {
      value.set_boolean(!left_value.is_null());
    }
  } else if (right_value.attr_type() == AttrType::BOOLEANS) {
    if (comp_ == CompOp::IS) {
      value.set_boolean(left_value.get_boolean() == right_value.get_boolean());
    } else {
      value.set_boolean(left_value.get_boolean() != right_value.get_boolean());
    }
  } else {
    LOG_WARN("right expression of IS must be a boolean constant");
    return RC::INVALID_ARGUMENT;
  }
  return RC::SUCCESS;
}
////////////////////////////////////////////////////////////////////////////////

enum class LIKE_RESULT
{
  LIKE_TRUE,   // like 匹配成功
  LIKE_FALSE,  // like 匹配失败
  LIKE_ABORT   // 内部使用,表示 s 用完而 pattern 还有剩
};

static LIKE_RESULT string_like_internal(const char *s, const char *p)
{
  int sLen = strlen(s);
  int pLen = strlen(p);
  if (pLen == 1 && p[0] == '%') {
    return LIKE_RESULT::LIKE_TRUE;
  }
  while (pLen > 0 && sLen > 0) {
    if (*p == '\\') {  // 转移后可以匹配元字符
      p++;
      pLen--;
      if (*p != *s) {
        return LIKE_RESULT::LIKE_FALSE;
      }
    } else if (*p == '%') {
      p++;
      pLen--;

      // 滑过 % 和 _ ，找到之后的第一个普通字符
      while (pLen > 0) {
        if (*p == '%') {
          p++;
          pLen--;
        } else if (*p == '_') {
          if (sLen <= 0) {
            return LIKE_RESULT::LIKE_ABORT;
          }
          p++;
          pLen--;
          s++;
          sLen--;
        } else {
          break;
        }
      }
      // pattern 以 % 和 _ 结尾
      if (pLen <= 0) {
        return LIKE_RESULT::LIKE_TRUE;
      }
      char firstpat;
      if (*p == '\\') {
        ASSERT(pLen < 2, "LIKE pattern must not end with escape character");
        firstpat = p[1];
      } else {
        firstpat = *p;
      }

      // 找到 s 中 firstpat 开头的子串，递归匹配
      while (sLen > 0) {
        if (*s == firstpat) {
          LIKE_RESULT matched = string_like_internal(s, p);
          // 返回 LIKE_TRUE 匹配成功,直接返回
          // 返回 LIKE_ABORT 此处 s 的长度已经不足 pattern 完成匹配, 下一次循环 s 的长度更短,不必继续递归.快速终止匹配
          // 返回 LIKE_FALSE 匹配失败,尝试下一个firstpat 开头的子串
          if (matched != LIKE_RESULT::LIKE_FALSE) {
            return matched;
          }
        }
        s++;
        sLen--;
      }
      // 此处 sLen < 0， 说明 s 用尽而 pattern 有剩余
      return LIKE_RESULT::LIKE_ABORT;

    } else if (*p == '_') {
      // nop
    } else if (*p != *s) {
      return LIKE_RESULT::LIKE_FALSE;
    }
    p++;
    pLen--;
    s++;
    sLen--;
  }

  // pattern 已经结束, s 还没有结束
  if (sLen > 0) {
    return LIKE_RESULT::LIKE_FALSE;
  }

  // s 已经结束，pattern 中还剩若干 % ，可以匹配空字符
  // 此逻辑也能处理 pattern 也结束的情况( pattern 还剩零个 % )
  while (pLen > 0 && *p == '%') {
    p++;
    pLen--;
  }
  if (pLen <= 0) {
    return LIKE_RESULT::LIKE_TRUE;
  }

  // pLen > 0, s 已经结束, pattern 去掉 % 后仍有其他字符, 无法匹配
  return LIKE_RESULT::LIKE_ABORT;
}

bool string_like(const char *s, const char *p) { return string_like_internal(s, p) == LIKE_RESULT::LIKE_TRUE; }

LikeExpr::LikeExpr(CompOp comp_op, std::unique_ptr<Expression> left, std::unique_ptr<Expression> right)
    : comp_(comp_op), left_(std::move(left)), right_(std::move(right))
{}

RC LikeExpr::get_value(const Tuple &tuple, Value &value) const
{
  RC rc = RC::SUCCESS;
  if (comp_ != CompOp::LIKE_OP && comp_ != CompOp::NOT_LIKE_OP) {
    LOG_WARN("unsupported LIKE expression. %d", comp_);
    return RC::INTERNAL;
  }
  if (right_->type() != ExprType::VALUE) {
    LOG_WARN("right expression of LIKE must be a char constant");
    return RC::INVALID_ARGUMENT;
  }
  Value left_value;
  Value right_value;
  rc = left_->get_value(tuple, left_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of left expression. rc=%s", strrc(rc));
    return rc;
  }
  if (left_value.attr_type() != AttrType::CHARS) {
    LOG_ERROR("value type %s doesn't support 'like'", attr_type_to_string(left_value.attr_type()));
    return RC::UNIMPLEMENTED;
  }
  rc = right_->get_value(tuple, right_value);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to get value of right expression. rc=%s", strrc(rc));
    return rc;
  }
  if (right_value.attr_type() != AttrType::CHARS) {
    LOG_ERROR("value type %s doesn't support 'like'", attr_type_to_string(right_value.attr_type()));
    return RC::UNIMPLEMENTED;
  }
  const string &left_str  = left_value.get_string();
  const string &right_str = right_value.get_string();
  bool          is_like   = (comp() == CompOp::LIKE_OP);
  if (string_like(left_str.c_str(), right_str.c_str())) {
    value.set_boolean(is_like);
  } else {
    value.set_boolean(!is_like);
  }
  return RC::SUCCESS;
}
////////////////////////////////////////////////////////////////////////////////
SubqueryExpr::SubqueryExpr(ParsedSqlNode *sub_query_sn) : sub_query_sn_(sub_query_sn) {}

void SubqueryExpr::set_logical_operator(std::unique_ptr<LogicalOperator> logical_operator)
{
  logical_operator_ = std::move(logical_operator);
}
void SubqueryExpr::set_physical_operator(std::unique_ptr<PhysicalOperator> physical_operator)
{
  physical_operator_ = std::move(physical_operator);
}

void                               SubqueryExpr::set_stmt(std::unique_ptr<SelectStmt> stmt) { stmt_ = std::move(stmt); }
ParsedSqlNode                     *SubqueryExpr::sub_query_sn() { return sub_query_sn_; }
std::unique_ptr<SelectStmt>       &SubqueryExpr::stmt() { return stmt_; }
std::unique_ptr<LogicalOperator>  &SubqueryExpr::logical_operator() { return logical_operator_; }
std::unique_ptr<PhysicalOperator> &SubqueryExpr::physical_operator() { return physical_operator_; }

RC SubqueryExpr::check_sub_select_legal(Db *db)
{
  // 允许的检查属性 ub_field 、* 、ub_aggregation
  UnboundFieldExpr *field_expr = nullptr;
  StarExpr         *star_expr  = nullptr;
  for (auto &expr : sub_query_sn_->selection.expressions) {
    // 得到ub_field
    LOG_DEBUG("the type of subquery expression is %s", expr_type_to_string(expr->type()).c_str());
    if (field_expr != nullptr) {
      // 当左子查询的属性不止一个时，报错
      LOG_WARN("invalid subquery attributes. It should be only one");
      return RC::INVALID_ARGUMENT;
    }
    if (expr->type() == ExprType::UNBOUND_FIELD) {
      field_expr = static_cast<UnboundFieldExpr *>(expr.get());
    } else if (expr->type() == ExprType::STAR) {
      star_expr = static_cast<StarExpr *>(expr.get());
    }
  }
  if (field_expr != nullptr && star_expr != nullptr) {
    LOG_WARN("star_expr and unbounded_field_expr cannot be used together in subquery");
    return RC::INVALID_ARGUMENT;
  }
  // check star的表中属性是否大于1
  if (star_expr != nullptr) {
    int fields_num = 0;
    for (size_t j = 0; j < sub_query_sn_->selection.relations.size(); ++j) {
      const char *table_name = sub_query_sn_->selection.relations[j].relation_name.c_str();
      if (nullptr == table_name) {
        LOG_WARN("invalid argument. relation name is null. index=%d", j);
        return RC::INVALID_ARGUMENT;
      }
      Table *table = db->find_table(table_name);
      if (nullptr == table) {
        LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
        return RC::SCHEMA_TABLE_NOT_EXIST;
      }
      fields_num += table->table_meta().field_num();
    }
    if (fields_num != 1) {
      LOG_WARN("invalid subquery attributes");
      return RC::INVALID_ARGUMENT;
    }
  }
  return RC::SUCCESS;
}

RC SubqueryExpr::get_value_with_trx(const Tuple &tuple, Value &value, Trx *trx) const
{
  RC rc = RC::SUCCESS;

  if (logical_operator_ == nullptr && physical_operator_ == nullptr) {
    return RC::RECORD_EOF;
  }

  if (physical_operator_ == nullptr) {
    LOG_WARN("physical operator is null");
    return RC::INVALID_ARGUMENT;
  }

  trx_ = trx;

  auto *tuple__ = const_cast<Tuple *>(&tuple);
  if (!is_open_) {
    rc = open_physical_operator(tuple__);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to open physical operator. rc=%s", strrc(rc));
      return rc;
    }
  }

  // 开始执行物理操作
  rc = physical_operator_->next();
  if (rc != RC::SUCCESS) {
    if (rc != RC::RECORD_EOF) {
      close_physical_operator();  // 关闭子查询算子
      LOG_PANIC("failed to get next tuple. rc=%s", strrc(rc));
      return rc;
    }
    // EOF
    rc = close_physical_operator();
    if (rc == RC::SUCCESS) {
      rc = RC::RECORD_EOF;
    } else {
      LOG_PANIC("failed to close physical operator. rc=%s", strrc(rc));
    }
    return rc;
  }
  auto tuple_ = physical_operator_->current_tuple();
  // 子查询的结果中，tuple 只能有一个 cell。这里应该不会出现tuple的cell数大于1的情况（在上层就已经排除了）
  if (tuple_->cell_num() > 1) {
    LOG_WARN("tuple cell count is not 1");
    close_physical_operator();  // 关闭子查询算子
    return RC::INVALID_ARGUMENT;
  }
  if (tuple_->cell_num() == 0) {
    LOG_WARN("A warn from SubqueryExpr: tuple cell count is 0");
  }
  tuple_->cell_at(0, value);
  return rc;
}

RC SubqueryExpr::get_value(const Tuple &tuple, Value &value) const { return get_value_with_trx(tuple, value, nullptr); }
RC SubqueryExpr::open_physical_operator(Tuple *outer_tuple) const
{
  if (physical_operator_ == nullptr) {
    LOG_WARN("physical operator is null");
    return RC::INVALID_ARGUMENT;
  }
  // 将外层的 tuple 传递给子查询算子，以达到查外层表的目的
  physical_operator_->set_outer_tuple(outer_tuple);
  RC rc = physical_operator_->open(trx_);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open physical operator. rc=%s", strrc(rc));
  } else {
    is_open_ = true;
  }
  return rc;
}

RC SubqueryExpr::close_physical_operator() const
{
  if (physical_operator_ == nullptr) {
    LOG_WARN("physical operator is null");
    return RC::INVALID_ARGUMENT;
  }
  RC rc = physical_operator_->close();
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to close physical operator. rc=%s", strrc(rc));
  } else {
    is_open_ = false;
  }
  return rc;
}
////////////////////////////////////////////////////////////////////////////////
RC ValueListExpr::get_value(const Tuple &tuple, Value &value) const
{
  if (index_ >= values_.size()) {
    index_ = 0;
    return RC::RECORD_EOF;
  }
  value = values_[index_++];
  return RC::SUCCESS;
}