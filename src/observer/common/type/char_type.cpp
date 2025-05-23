/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "common/lang/comparator.h"
#include "common/log/log.h"
#include "common/type/char_type.h"
#include "common/value.h"

int CharType::compare(const Value &left, const Value &right) const
{
  // for char and float cast test case
  if (left.attr_type() == AttrType::CHARS && (right.attr_type() == AttrType::FLOATS || right.attr_type() == AttrType::INTS)) {
    float left_val  = left.get_float();
    float right_val = right.get_float();
    return common::compare_float((void *)&left_val, (void *)&right_val);
  }
  ASSERT(left.attr_type() == AttrType::CHARS && right.attr_type() == AttrType::CHARS, "invalid type");
  return common::compare_string(
      (void *)left.value_.pointer_value_, left.length_, (void *)right.value_.pointer_value_, right.length_);
}

RC CharType::set_value_from_str(Value &val, const string &data) const
{
  val.set_string(data.c_str());
  return RC::SUCCESS;
}

static bool check_valid_date(int y, int m, int d)
{
  static const int days[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  bool             leap   = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
  return (y > 0 && y <= 9999) && (m > 0 && m <= 12) && (d > 0 && d <= (days[m] + (m == 2 && leap ? 1 : 0)));
}

RC CharType::cast_to(const Value &val, AttrType type, Value &result) const
{
  switch (type) {
    case AttrType::DATES: {
      static int y, m, d;
      if (sscanf(val.value_.pointer_value_, "%d-%d-%d", &y, &m, &d) != 3) {
        return RC::INVALID_ARGUMENT;
      }
      if (!check_valid_date(y, m, d)) {
        return RC::INVALID_ARGUMENT;
      }
      result.attr_type_ = AttrType::DATES;
      result.set_date(y * 10000 + m * 100 + d);
    } break;
    default: return RC::UNIMPLEMENTED;
  }
  return RC::SUCCESS;
}

int CharType::cast_cost(AttrType type)
{
  if (type == AttrType::CHARS) {
    return 0;
  } else if (type == AttrType::DATES) {
    return 1;
  }
  return INT32_MAX;
}

RC CharType::to_string(const Value &val, string &result) const
{
  stringstream ss;
  ss << val.value_.pointer_value_;
  result = ss.str();
  return RC::SUCCESS;
}