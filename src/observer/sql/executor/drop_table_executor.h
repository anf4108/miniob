
#pragma once

#include "common/sys/rc.h"

class SQLStageEvent;

/**
 * @brief 删除表的执行器
 * @ingroup Executor
 */
class DropTableExecutor
{
public:
  DropTableExecutor()          = default;
  virtual ~DropTableExecutor() = default;
  /**
   * @brief 执行删除表的操作
   * @param sql_event SQL事件
   * @return 执行结果
   */
  RC execute(SQLStageEvent *sql_event);
};