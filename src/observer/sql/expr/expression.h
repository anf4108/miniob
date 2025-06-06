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

#pragma once

#include "common/lang/string.h"
#include "common/lang/memory.h"
#include "common/value.h"
#include "storage/field/field.h"
#include "sql/expr/aggregator.h"
#include "storage/common/chunk.h"
#include "storage/db/db.h"

class Tuple;
class SelectStmt;
class ParsedSqlNode;
class LogicalOperator;
class PhysicalOperator;
class Db;
/**
 * @defgroup Expression
 * @brief 表达式
 */

/**
 * @brief 表达式类型
 * @ingroup Expression
 */
enum class ExprType
{
  NONE,
  STAR,                 ///< 星号，表示所有字段
  UNBOUND_FIELD,        ///< 未绑定的字段，需要在resolver阶段解析为FieldExpr
  UNBOUND_AGGREGATION,  ///< 未绑定的聚合函数，需要在resolver阶段解析为AggregateExpr

  FIELD,         ///< 字段。在实际执行时，根据行数据内容提取对应字段的值
  VALUE,         ///< 常量值
  CAST,          ///< 需要做类型转换的表达式
  COMPARISON,    ///< 需要做比较的表达式
  CONJUNCTION,   ///< 多个表达式使用同一种关系(AND或OR)来联结
  ARITHMETIC,    ///< 算术运算
  AGGREGATION,   ///< 聚合运算
  SYS_FUNCTION,  ///< 系统函数调用
  IS,            ///< 判断是否为NULL or Bool
  LIKE,          //< 判断是否LIKE某个Pattern
  VALUES,        ///< 常量值列表
  SUB_QUERY,     ///< 子查询
};

/// 用于debug
std::string expr_type_to_string(ExprType type);
/**
 * @brief 表达式的抽象描述
 * @ingroup Expression
 * @details 在SQL的元素中，任何需要得出值的元素都可以使用表达式来描述
 * 比如获取某个字段的值、比较运算、类型转换
 * 当然还有一些当前没有实现的表达式，比如算术运算。
 *
 * 通常表达式的值，是在真实的算子运算过程中，拿到具体的tuple后
 * 才能计算出来真实的值。但是有些表达式可能就表示某一个固定的
 * 值，比如ValueExpr。
 *
 * TODO 区分unbound和bound的表达式
 */
class Expression
{
public:
  Expression()          = default;
  virtual ~Expression() = default;

  /**
   * @brief 判断两个表达式是否相等
   */
  virtual bool equal(const Expression &other) const { return false; }
  /**
   * @brief 根据具体的tuple，来计算当前表达式的值。tuple有可能是一个具体某个表的行数据
   */
  virtual RC get_value(const Tuple &tuple, Value &value) const = 0;

  /**
   * @brief 在没有实际运行的情况下，也就是无法获取tuple的情况下，尝试获取表达式的值
   * @details 有些表达式的值是固定的，比如ValueExpr，这种情况下可以直接获取值
   */
  virtual RC try_get_value(Value &value) const { return RC::UNIMPLEMENTED; }

  /**
   * @brief 从 `chunk` 中获取表达式的计算结果 `column`
   */
  virtual RC get_column(Chunk &chunk, Column &column) { return RC::UNIMPLEMENTED; }

  /**
   * @brief 表达式的类型
   * 可以根据表达式类型来转换为具体的子类
   */
  virtual ExprType type() const = 0;

  /**
   * @brief 表达式值的类型
   * @details 一个表达式运算出结果后，只有一个值
   */
  virtual AttrType value_type() const = 0;

  /**
   * @brief 表达式值的长度
   */
  virtual int value_length() const { return -1; }

  /**
   * @brief 表达式的名字，比如是字段名称，或者用户在执行SQL语句时输入的内容
   */
  virtual const char *name() const { return name_.c_str(); }
  virtual void        set_name(string name) { name_ = name; }

  /**
   * @brief 表达式的别名
   * @details 在SQL语句中，用户可以给表达式起一个别名
   */
  virtual string alias() const { return alias_; }
  const char    *alias_c_str() const { return alias_.c_str(); }
  virtual void   set_alias(string alias) { alias_ = alias; }

  virtual string table_alias() const { return table_alias_; }
  virtual void   set_table_alias(string table_alias) { table_alias_ = table_alias; }
  /**
   * @brief 表达式在下层算子返回的 chunk 中的位置
   */
  virtual int  pos() const { return pos_; }
  virtual void set_pos(int pos) { pos_ = pos; }

  /**
   * @brief 用于 ComparisonExpr 获得比较结果 `select`。
   */
  virtual RC eval(Chunk &chunk, vector<uint8_t> &select) { return RC::UNIMPLEMENTED; }

protected:
  /**
   * @brief 表达式在下层算子返回的 chunk 中的位置
   * @details 当 pos_ = -1 时表示下层算子没有在返回的 chunk 中计算出该表达式的计算结果，
   * 当 pos_ >= 0时表示在下层算子中已经计算出该表达式的值（比如聚合表达式），且该表达式对应的结果位于
   * chunk 中 下标为 pos_ 的列中。
   */
  int pos_ = -1;

private:
  string name_;
  string alias_;        ///< 别名
  string table_alias_;  ///< 表别名
};

class StarExpr : public Expression
{
public:
  StarExpr() : table_name_() {}
  StarExpr(const char *table_name) : table_name_(table_name) {}
  virtual ~StarExpr() = default;

  ExprType type() const override { return ExprType::STAR; }
  AttrType value_type() const override { return AttrType::UNDEFINED; }

  RC get_value(const Tuple &tuple, Value &value) const override { return RC::UNIMPLEMENTED; }  // 不需要实现

  const char *table_name() const { return table_name_.c_str(); }
  void        set_table_name(const char *table_name) { table_name_ = table_name; }

private:
  string table_name_;
};

class UnboundFieldExpr : public Expression
{
public:
  UnboundFieldExpr(const string table_name, const string field_name) : table_name_(table_name), field_name_(field_name)
  {}

  virtual ~UnboundFieldExpr() = default;

  ExprType type() const override { return ExprType::UNBOUND_FIELD; }
  AttrType value_type() const override { return AttrType::UNDEFINED; }

  RC get_value(const Tuple &tuple, Value &value) const override { return RC::INTERNAL; }

  const char *table_name() const { return table_name_.c_str(); }
  const char *field_name() const { return field_name_.c_str(); }

  void set_field_name(const std::string &field_name) { field_name_ = field_name; }
  void set_table_name(const std::string &table_name) { table_name_ = table_name; }

private:
  string table_name_;
  string field_name_;
};

/**
 * @brief 字段表达式
 * @ingroup Expression
 */
class FieldExpr : public Expression
{
public:
  FieldExpr() = default;
  FieldExpr(const Table *table, const FieldMeta *field) : field_(table, field) {}
  FieldExpr(const Field &field, const char *table_name = nullptr) : field_(field), table_name_(table_name) {}

  virtual ~FieldExpr() = default;

  bool equal(const Expression &other) const override;

  ExprType type() const override { return ExprType::FIELD; }
  AttrType value_type() const override { return field_.attr_type(); }
  int      value_length() const override { return field_.meta()->len(); }

  Field &field() { return field_; }

  const Field &field() const { return field_; }

  const char *table_name() const { return field_.table_name(); }
  const char *field_name() const { return field_.field_name(); }
  const char *try_get_table_name_in_multi_table_query() const { return table_name_; }

  RC get_column(Chunk &chunk, Column &column) override;

  RC get_value(const Tuple &tuple, Value &value) const override;

private:
  Field       field_;
  const char *table_name_ = nullptr;  ///< 用于多表join时的表名
};

/**
 * @brief 常量值表达式
 * @ingroup Expression
 */
class ValueExpr : public Expression
{
public:
  ValueExpr() = default;
  explicit ValueExpr(const Value &value) : value_(value) {}

  virtual ~ValueExpr() = default;

  bool equal(const Expression &other) const override;

  RC get_value(const Tuple &tuple, Value &value) const override;
  RC get_column(Chunk &chunk, Column &column) override;
  RC try_get_value(Value &value) const override
  {
    value = value_;
    return RC::SUCCESS;
  }

  ExprType type() const override { return ExprType::VALUE; }
  AttrType value_type() const override { return value_.attr_type(); }
  int      value_length() const override { return value_.length(); }

  void         get_value(Value &value) const { value = value_; }
  const Value &get_value() const { return value_; }

private:
  Value value_;
};

/**
 * @brief 类型转换表达式
 * @ingroup Expression
 */
class CastExpr : public Expression
{
public:
  CastExpr(unique_ptr<Expression> child, AttrType cast_type);
  virtual ~CastExpr();

  ExprType type() const override { return ExprType::CAST; }

  RC get_value(const Tuple &tuple, Value &value) const override;

  RC try_get_value(Value &value) const override;

  AttrType value_type() const override { return cast_type_; }

  unique_ptr<Expression> &child() { return child_; }

private:
  RC cast(const Value &value, Value &cast_value) const;

private:
  unique_ptr<Expression> child_;      ///< 从这个表达式转换
  AttrType               cast_type_;  ///< 想要转换成这个类型
};

/**
 * @brief 比较表达式
 * @ingroup Expression
 */
class ComparisonExpr : public Expression
{
public:
  ComparisonExpr(CompOp comp, unique_ptr<Expression> left, unique_ptr<Expression> right);
  virtual ~ComparisonExpr();

  ExprType type() const override { return ExprType::COMPARISON; }
  RC       get_value(const Tuple &tuple, Value &value) const override;
  AttrType value_type() const override { return AttrType::BOOLEANS; }
  CompOp   comp() const { return comp_; }

  /**
   * @brief 根据 ComparisonExpr 获得 `select` 结果。
   * select 的长度与chunk 的行数相同，表示每一行在ComparisonExpr 计算后是否会被输出。
   */
  RC eval(Chunk &chunk, vector<uint8_t> &select) override;

  unique_ptr<Expression> &left() { return left_; }
  unique_ptr<Expression> &right() { return right_; }

  /**
   * 尝试在没有tuple的情况下获取当前表达式的值
   * 在优化的时候，可能会使用到
   */
  RC try_get_value(Value &value) const override;

  /**
   * compare the two tuple cells
   * @param value the result of comparison
   */
  RC compare_value(const Value &left, const Value &right, bool &value) const;

  template <typename T>
  RC compare_column(const Column &left, const Column &right, vector<uint8_t> &result) const;

private:
  CompOp                 comp_;
  unique_ptr<Expression> left_;
  unique_ptr<Expression> right_;
};

/**
 * @brief 联结表达式
 * @ingroup Expression
 * 多个表达式使用同一种关系(AND或OR)来联结
 * 当前miniob仅有AND操作
 */
class ConjunctionExpr : public Expression
{
public:
  enum class Type
  {
    AND,
    OR,
  };

public:
  ConjunctionExpr(Type type, vector<unique_ptr<Expression>> &children);
  virtual ~ConjunctionExpr() = default;

  ExprType type() const override { return ExprType::CONJUNCTION; }
  AttrType value_type() const override { return AttrType::BOOLEANS; }
  RC       get_value(const Tuple &tuple, Value &value) const override;

  Type conjunction_type() const { return conjunction_type_; }

  vector<unique_ptr<Expression>> &children() { return children_; }

private:
  Type                           conjunction_type_;
  vector<unique_ptr<Expression>> children_;
};

/**
 * @brief 算术表达式
 * @ingroup Expression
 */
class ArithmeticExpr : public Expression
{
public:
  enum class Type
  {
    ADD,
    SUB,
    MUL,
    DIV,
    NEGATIVE,
  };

public:
  ArithmeticExpr(Type type, Expression *left, Expression *right);
  ArithmeticExpr(Type type, unique_ptr<Expression> left, unique_ptr<Expression> right);
  virtual ~ArithmeticExpr() = default;

  bool     equal(const Expression &other) const override;
  ExprType type() const override { return ExprType::ARITHMETIC; }

  AttrType value_type() const override;
  int      value_length() const override
  {
    if (!right_) {
      return left_->value_length();
    }
    return 4;  // sizeof(float) or sizeof(int)
  };

  RC get_value(const Tuple &tuple, Value &value) const override;

  RC get_column(Chunk &chunk, Column &column) override;

  RC try_get_value(Value &value) const override;

  Type arithmetic_type() const { return arithmetic_type_; }

  unique_ptr<Expression> &left() { return left_; }
  unique_ptr<Expression> &right() { return right_; }

private:
  RC calc_value(const Value &left_value, const Value &right_value, Value &value) const;

  RC calc_column(const Column &left_column, const Column &right_column, Column &column) const;

  template <bool LEFT_CONSTANT, bool RIGHT_CONSTANT>
  RC execute_calc(const Column &left, const Column &right, Column &result, Type type, AttrType attr_type) const;

private:
  Type                   arithmetic_type_;
  unique_ptr<Expression> left_;
  unique_ptr<Expression> right_;
};

/**
 * @brief 未绑定的聚合表达式
 * @ingroup Expression
 * @details 该表达式在解析SQL语句时，无法确定具体的聚合函数
 */
class UnboundAggregateExpr : public Expression
{
public:
  UnboundAggregateExpr(const char *aggregate_name, Expression *child);
  virtual ~UnboundAggregateExpr() = default;

  ExprType type() const override { return ExprType::UNBOUND_AGGREGATION; }

  const char *aggregate_name() const { return aggregate_name_.c_str(); }

  unique_ptr<Expression> &child() { return child_; }

  RC       get_value(const Tuple &tuple, Value &value) const override { return RC::INTERNAL; }
  AttrType value_type() const override { return child_->value_type(); }

private:
  string                 aggregate_name_;
  unique_ptr<Expression> child_;
};

/**
 * @brief 聚合表达式
 * @ingroup Expression
 */
class AggregateExpr : public Expression
{
public:
  enum class Type
  {
    COUNT,
    SUM,
    AVG,
    MAX,
    MIN,
  };

public:
  AggregateExpr(Type type, Expression *child);
  AggregateExpr(Type type, unique_ptr<Expression> child);
  virtual ~AggregateExpr() = default;

  bool equal(const Expression &other) const override;

  ExprType type() const override { return ExprType::AGGREGATION; }

  AttrType value_type() const override { return child_->value_type(); }
  int      value_length() const override { return child_->value_length(); }

  RC get_value(const Tuple &tuple, Value &value) const override;

  RC get_column(Chunk &chunk, Column &column) override;

  Type aggregate_type() const { return aggregate_type_; }

  unique_ptr<Expression> &child() { return child_; }

  const unique_ptr<Expression> &child() const { return child_; }

  unique_ptr<Aggregator> create_aggregator() const;

public:
  static RC type_from_string(const char *type_str, Type &type);

private:
  Type                   aggregate_type_;
  unique_ptr<Expression> child_;
};

/**
 * @brief IS 表达式
 * @ingroup Expression
 * IS 表达式，用于判断是否为 bool or null，因此右边的表达式必须是一个常量，
 */
class IsExpr : public Expression
{
public:
  IsExpr(CompOp comp_op, std::unique_ptr<Expression> left, std::unique_ptr<Expression> right);
  virtual ~IsExpr() = default;

  ExprType type() const override { return ExprType::IS; }
  int      value_length() const override { return sizeof(bool); }
  RC       get_value(const Tuple &tuple, Value &value) const override;
  AttrType value_type() const override { return AttrType::BOOLEANS; }
  CompOp   comp() const { return comp_; }

  unique_ptr<Expression> &left() { return left_; }
  unique_ptr<Expression> &right() { return right_; }

private:
  CompOp                      comp_;  // 只允许 IS or IS_NOT
  std::unique_ptr<Expression> left_;
  std::unique_ptr<Expression> right_;
};

/**
 * @brief LIKE 表达式
 * @ingroup Expression
 * LIKE 表达式，用于判断是否为某个模式，因此右边的表达式必须是一个常量，
 */
class LikeExpr : public Expression
{
public:
  LikeExpr(CompOp comp_op, std::unique_ptr<Expression> left, std::unique_ptr<Expression> right);
  virtual ~LikeExpr() = default;

  ExprType type() const override { return ExprType::LIKE; }
  int      value_length() const override { return sizeof(bool); }
  RC       get_value(const Tuple &tuple, Value &value) const override;
  AttrType value_type() const override { return AttrType::BOOLEANS; }
  CompOp   comp() const { return comp_; }

  unique_ptr<Expression> &left() { return left_; }
  unique_ptr<Expression> &right() { return right_; }

private:
  CompOp                      comp_;  // 只允许 LIKE or NOT_LIKE
  std::unique_ptr<Expression> left_;
  std::unique_ptr<Expression> right_;
};

/**
 * @brief 系统函数表达式
 * @ingroup Expression
 */
class SysFunctionExpr : public Expression
{
public:
  SysFunctionExpr(SysFuncType sys_func_type, vector<unique_ptr<Expression>> &params)
      : sys_func_type_(sys_func_type), params_(std::move(params))
  {}
  SysFunctionExpr(SysFuncType sys_func_type, vector<Expression *> &params) : sys_func_type_(sys_func_type)
  {
    for (auto &param : params)
      params_.emplace_back(param);
  }
  virtual ~SysFunctionExpr() = default;

  ExprType type() const override { return ExprType::SYS_FUNCTION; }
  AttrType value_type() const override;

  RC get_func_length_value(const Tuple &tuple, Value &value) const;

  RC get_func_round_value(const Tuple &tuple, Value &value) const;

  RC get_func_date_format_value(const Tuple &tuple, Value &value) const;

  RC get_value(const Tuple &tuple, Value &value) const override
  {
    RC rc = RC::SUCCESS;
    switch (sys_func_type_) {
      case SysFuncType::LENGTH: {
        rc = get_func_length_value(tuple, value);
        break;
      }
      case SysFuncType::ROUND: {
        rc = get_func_round_value(tuple, value);
        break;
      }
      case SysFuncType::DATE_FORMAT: {
        rc = get_func_date_format_value(tuple, value);
        break;
      }
      default: {
        LOG_WARN("unsupported system function type. %d", sys_func_type_);
        rc = RC::INTERNAL;
        break;
      }
    }
    return rc;
  }

  RC try_get_func_length_value(Value &value) const;

  RC try_get_func_round_value(Value &value) const;

  RC try_get_func_date_format_value(Value &value) const;

  RC try_get_value(Value &value) const override
  {
    RC rc = RC::SUCCESS;
    LOG_DEBUG("try_get_value sys_func_type_ %d", sys_func_type_);
    switch (sys_func_type_) {
      case SysFuncType::LENGTH: {
        return try_get_func_length_value(value);
      }
      case SysFuncType::ROUND: {
        return try_get_func_round_value(value);
      }
      case SysFuncType::DATE_FORMAT: {
        return try_get_func_date_format_value(value);
      }
      default: {
        LOG_WARN("unsupported system function type. %d", sys_func_type_);
        return RC::INTERNAL;
      }
    }
    return rc;
  }

  SysFuncType sys_func_type() const { return sys_func_type_; }

  vector<unique_ptr<Expression>> &params() { return params_; }

  RC check_params_type_and_number() const;

private:
  SysFuncType                    sys_func_type_;
  vector<unique_ptr<Expression>> params_;
};

//
/**
 * @brief 子查询表达式
 * @ingroup Expression
 */

class SubqueryExpr : public Expression
{
public:
  SubqueryExpr(ParsedSqlNode *sub_query_sn);
  ExprType type() const override { return ExprType::SUB_QUERY; }
  AttrType value_type() const override { return AttrType::UNDEFINED; }
  RC       get_value(const Tuple &tuple, Value &value) const override;
  RC       get_value_with_trx(const Tuple &tuple, Value &value, Trx *trx = nullptr) const;
  RC       check_sub_select_legal(Db *db);  // 检查子查询属性是否正常

  void set_logical_operator(std::unique_ptr<LogicalOperator> logical_operator);
  void set_physical_operator(std::unique_ptr<PhysicalOperator> physical_operator);

  // 子算子的 open 和 close 逻辑由外部控制
  RC                                 open_physical_operator(Tuple *outer_tuple) const;
  RC                                 close_physical_operator() const;
  void                               set_stmt(std::unique_ptr<SelectStmt> stmt);
  ParsedSqlNode                     *sub_query_sn();
  std::unique_ptr<SelectStmt>       &stmt();
  std::unique_ptr<LogicalOperator>  &logical_operator();
  std::unique_ptr<PhysicalOperator> &physical_operator();

private:
  ParsedSqlNode                    *sub_query_sn_;  // SqlNode
  std::unique_ptr<SelectStmt>       stmt_;
  mutable bool                      is_open_ = false;
  mutable Trx                      *trx_;
  std::unique_ptr<LogicalOperator>  logical_operator_;
  std::unique_ptr<PhysicalOperator> physical_operator_;
};

/**
 * @brief 常量值列表表达式，用于 IN/NOT IN/EXISTS/NOT EXISTS
 * @ingroup Expression
 */
class ValueListExpr : public Expression
{
public:
  ValueListExpr() = default;
  explicit ValueListExpr(const std::vector<Value> &values) : values_(values) {}

  virtual ~ValueListExpr() = default;

  RC get_value(const Tuple &tuple, Value &value) const override;
  RC try_get_value(Value &value) const override
  {
    /// 何时会try_get? 在ComparisonExpr的优化阶段会尝试try_get_value，如果可以判断出恒真or恒假
    /// 执行简化规则, 此处的try_get_value无所谓
    value = values_[0];
    return RC::SUCCESS;
  }

  ExprType type() const override { return ExprType::VALUES; }

  AttrType value_type() const override { return values_[0].attr_type(); }

  void set_index(int index) { index_ = index; }

  const std::vector<Value> &get_values() const { return values_; }

private:
  std::vector<Value> values_;
  // 即使在一个被声明为const的成员函数内，这个成员变量也可以被修改。
  mutable size_t index_ = 0;
};
