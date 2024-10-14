#pragma once
#include "common/rc.h"

class SQLStageEvent;


class DropTableExecutor {
public:
    RC execute(SQLStageEvent* sql_event);
};
