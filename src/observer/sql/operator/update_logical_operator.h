#include "sql/operator/logical_operator.h"
#include "storage/field/field.h"

class UpdateLogicalOperator : public LogicalOperator
{
public:
  LogicalOperatorType type() const;
  UpdateLogicalOperator(Table *table, Field field, Value value);

  Field &update_field();
  Value &value();
  Table *table();

private:
  Field  update_field_;
  Value  value_;
  Table *table_;
};