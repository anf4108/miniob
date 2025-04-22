#include "sql/operator/physical_operator.h"
#include "sql/optimizer/physical_plan_generator.h"
class UpdatePhysicalOperator : public PhysicalOperator
{
public:
  UpdatePhysicalOperator(Field field, Value value, Table *table) : update_field_(field), value_(value), table_(table) {}

private:
  PhysicalOperatorType type() const override { return PhysicalOperatorType::UPDATE; }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  virtual Tuple *current_tuple() override { return nullptr; }

  friend class PhysicalPlanGenerator;

private:
  RC insert(vector<char>&v, RID& rid);
  RC insert_all(vector<vector<char>> &v);
  RC remove_all(const vector<RID>&rids);
  RC update(vector<char> v, RID &rid);

private:
  Field update_field_;
  Value value_;
  Table *table_;
};