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
// Created by Soulter on 2024/11/5.
//

#pragma once

#include "common/rc.h"
#include "event/sql_event.h"

class SQLStageEvent;

/**
 * @brief 创建视图的执行器
 * @ingroup Executor
 */
class CreateViewExecutor
{
public:
  CreateViewExecutor()          = default;
  virtual ~CreateViewExecutor() = default;

  RC execute(SQLStageEvent *sql_event);
  void init_sys_view_table_attr_infos(std::vector<AttrInfoSqlNode> &attr_infos);
  void make_view_values(std::vector<Value> &values, const std::vector<std::string> &attrs_name, const std::string &view_name, const std::string &view_definition, bool is_updatable);
};