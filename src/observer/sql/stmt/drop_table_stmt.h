#pragma once

#include "sql/stmt/stmt.h"

/**
 * @brief 删除表的语句
 * @ingroup Statement
 */
class DropTableStmt : public Stmt
{
public:
  DropTableStmt(const string &table_name) : table_name_(table_name) {}

  virtual ~DropTableStmt() = default;

  StmtType type() const override { return StmtType::DROP_TABLE; }

  const string &table_name() const { return table_name_; }

  static RC create(Db *db, const DropTableSqlNode &create_table, Stmt *&stmt);

private:
  string table_name_;
};
