#include "sql/operator/update_logical_operator.h"

LogicalOperatorType UpdateLogicalOperator::type() const { return LogicalOperatorType::UPDATE; }

UpdateLogicalOperator::UpdateLogicalOperator(Table *table, Field field, Value value)
    : update_field_(field), value_(value), table_(table)
{}

Field &UpdateLogicalOperator::update_field() { return update_field_; }

Value &UpdateLogicalOperator::value() { return value_; }

Table *UpdateLogicalOperator::table() { return table_; }
