#pragma once

#include "common/rc.h"

class SQLStageEvent;

class CreateVectorIndexExecutor
{
public:
  CreateVectorIndexExecutor()          = default;
  virtual ~CreateVectorIndexExecutor() = default;

  RC execute(SQLStageEvent *sql_event);
};