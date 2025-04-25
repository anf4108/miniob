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
// Created by Wangyunlai on 2024/05/29.
//

#include "sql/expr/aggregator.h"
#include "common/log/log.h"

RC SumAggregator::accumulate(const Value &value)
{
  // 如果是NULL值，直接跳过
  if (value.is_null()) {
    return RC::SUCCESS;
  }
  // 如果是第一次累加
  if (value_.attr_type() == AttrType::UNDEFINED) {
    value_ = value;
    return RC::SUCCESS;
  }

  // really????
  ASSERT(value.attr_type() == value_.attr_type(), "type mismatch. value type: %s, value_.type: %s", 
        attr_type_to_string(value.attr_type()), attr_type_to_string(value_.attr_type()));

  RC rc = Value::add(value, value_, value_);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("failed to add value. rc=%s", strrc(rc));
    return rc;
  }
  return RC::SUCCESS;
}

RC SumAggregator::evaluate(Value &result)
{
  if (value_.attr_type() == AttrType::UNDEFINED) {
    result.set_null();
    return RC::SUCCESS;
  }
  result = value_;
  return RC::SUCCESS;
}

RC AvgAggregator::accumulate(const Value &value)
{
  if (value.is_null()) {
    return RC::SUCCESS;
  }
  if (value_.attr_type() == AttrType::UNDEFINED) {
    value_ = value;
    count_ = 1;
    return RC::SUCCESS;
  }

  Value sum_value;
  // 设置结果类型，暂时默认二者相同
  sum_value.set_type(value_.attr_type());
  RC rc = Value::add(value, value_, sum_value);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("failed to add value. rc=%s", strrc(rc));
    return rc;
  }
  // 防止算术操作状态影响实际结果
  value_ = sum_value;
  count_++;
  return RC::SUCCESS;
}

RC AvgAggregator::evaluate(Value &result)
{
  if (value_.attr_type() == AttrType::UNDEFINED) {
    result.set_null();
    return RC::SUCCESS;
  }
  Value divisor = Value(count_);
  result.set_type(AttrType::FLOATS);
  Value::divide(value_, divisor, result);
  return RC::SUCCESS;
}

RC CountAggregator::accumulate(const Value &value)
{
  LOG_DEBUG("count is %ld", count_);
  if (value.is_null()) {
    return RC::SUCCESS;
  }
  count_++;
  return RC::SUCCESS;
}

RC CountAggregator::evaluate(Value &result)
{
  // count聚合函数视最终结果的NULL值为0
  result = Value(count_);
  return RC::SUCCESS;
}

RC MaxAggregator::accumulate(const Value &value)
{
  if (value.is_null()) {
    return RC::SUCCESS;
  }
  if (value_.is_null()) {
    value_ = value;
    return RC::SUCCESS;
  }
  if (value_.attr_type() == AttrType::UNDEFINED) {
    value_ = value;
    return RC::SUCCESS;
  }

  if (value.compare(value_) > 0) {
    value_ = value;
  }
  return RC::SUCCESS;
}

RC MaxAggregator::evaluate(Value &result)
{
  if (value_.attr_type() == AttrType::UNDEFINED) {
    result.set_null();
    return RC::SUCCESS;
  }
  result = value_;
  return RC::SUCCESS;
}

RC MinAggregator::accumulate(const Value &value)
{
  if (value.is_null()) {
    return RC::SUCCESS;
  }
  if (value_.is_null()) {
    value_ = value;
    return RC::SUCCESS;
  }
  if (value_.attr_type() == AttrType::UNDEFINED) {
    value_ = value;
    return RC::SUCCESS;
  }

  if (value.compare(value_) < 0) {
    value_ = value;
  }
  return RC::SUCCESS;
}

RC MinAggregator::evaluate(Value &result)
{
  if (value_.attr_type() == AttrType::UNDEFINED) {
    result.set_null();
    return RC::SUCCESS;
  }
  result = value_;
  return RC::SUCCESS;
}
