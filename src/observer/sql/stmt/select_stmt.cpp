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
// Created by Wangyunlai on 2022/6/6.
//

#include "sql/stmt/select_stmt.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "sql/expr/expression.h"
#include "sql/stmt/filter_stmt.h"
#include "storage/db/db.h"
#include "storage/table/table.h"
#include "sql/parser/expression_binder.h"
#include <memory>

using namespace std;
using namespace common;

SelectStmt::~SelectStmt()
{
  if (nullptr != filter_stmt_) {
    delete filter_stmt_;
    filter_stmt_ = nullptr;
  }
}


RC SelectStmt::convert_alias_to_name(Expression *expr, 
std::shared_ptr<std::unordered_map<string, string>> alias2name,
std::shared_ptr<std::unordered_map<string, string>> field_alias2name) {
   if (expr->type() == ExprType::VALUE || 
   expr->type() == ExprType::SUB_QUERY || 
   expr->type() == ExprType::SPECIAL_PLACEHOLDER ||
   expr->type() == ExprType::VALUES ||
   expr->type() == ExprType::STAR ||
   expr->type() == ExprType::VECTOR_DISTANCE_EXPR){
    // select * from table_alias_1 t1 where id in (select t2.id from table_alias_2 t2 where t2.col2 >= t1.col1);
    // subquery 单独处理
    return RC::SUCCESS;
  }
  if (expr->type() == ExprType::ARITHMETIC) {
    ArithmeticExpr *arith_expr = static_cast<ArithmeticExpr *>(expr);
    if (arith_expr->left() != nullptr) {
      RC rc = SelectStmt::convert_alias_to_name(arith_expr->left().get(), alias2name, field_alias2name);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to check parent relation");
        return rc;
      }
    }
    if (arith_expr->right() != nullptr) {
      RC rc = SelectStmt::convert_alias_to_name(arith_expr->right().get(), alias2name, field_alias2name);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to check parent relation");
        return rc;
      }
    }
    return RC::SUCCESS;
  }
  if (expr->type() == ExprType::UNBOUND_AGGREGATION) {
    UnboundAggregateExpr *aggre_expr = static_cast<UnboundAggregateExpr *>(expr);
    if (aggre_expr->child() == nullptr) {
      LOG_WARN("invalid aggre expr");
      return RC::INVALID_ARGUMENT;
    }
    RC rc = SelectStmt::convert_alias_to_name(aggre_expr->child().get(), alias2name, field_alias2name);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to check parent relation");
      return rc;
    }
    return rc;
  }
  
  if (expr->type() != ExprType::UNBOUND_FIELD) {
    LOG_WARN("convert_alias_to_name: invalid expr type: %d. It should be UnoundField.", expr->type());
  }
  auto ub_field_expr = static_cast<UnboundFieldExpr *>(expr);

  // 替换 field 的表的别名为真实的表名
  if (alias2name->find(ub_field_expr->table_name()) != alias2name->end()) {
    // 如果在 alias2name 中找到了，说明是别名，需要替换为真实的表名
    std::string true_table_name = (*alias2name)[ub_field_expr->table_name()];
    LOG_DEBUG("convert alias to name: %s -> %s",ub_field_expr->table_name(), true_table_name.c_str());
    ub_field_expr->set_table_alias(ub_field_expr->table_name());
    ub_field_expr->set_table_name(true_table_name);
  }

  // 替换 field 的别名为真实的字段名
  // MYSQL 中，WHERE 不能使用别名
  // if (field_alias2name != nullptr) {
  //   if (field_alias2name->find(ub_field_expr->field_name()) != field_alias2name->end()) {
  //     // 如果在 field_alias2name 中找到了，说明是别名，需要替换为真实的字段名
  //     std::string true_field_name = (*field_alias2name)[ub_field_expr->field_name()];
  //     LOG_DEBUG("convert alias(field) to name: %s -> %s",ub_field_expr->field_name(), true_field_name.c_str());
  //     ub_field_expr->set_alias(ub_field_expr->field_name());
  //     ub_field_expr->set_field_name(true_field_name);
  //   }
  // }

  return RC::SUCCESS;
}

RC SelectStmt::create(Db *db, SelectSqlNode &select_sql, Stmt *&stmt,
  std::shared_ptr<std::unordered_map<string, string>> name2alias,
  std::shared_ptr<std::unordered_map<string, string>> alias2name,
  std::shared_ptr<std::vector<string>> loaded_relation_names,
  std::shared_ptr<std::unordered_map<string, string>> field_alias2name
  )
{
  // loaded_relation_names 是有必要的。
  // name2alias 虽然也存了 relation names，但只存的是 alias 不为空时的 relation names。
  if (nullptr == db) {
    LOG_WARN("invalid argument. db is null");
    return RC::INVALID_ARGUMENT;
  }

  if (select_sql.expressions.empty()) {
    LOG_WARN("invalid argument. select attributes(exprs, technically) is empty");
    return RC::INVALID_ARGUMENT;
  }

  if (name2alias == nullptr) name2alias = std::make_shared<std::unordered_map<string, string>>();
  if (alias2name == nullptr) alias2name = std::make_shared<std::unordered_map<string, string>>();
  if (loaded_relation_names == nullptr) loaded_relation_names = std::make_shared<std::vector<string>>();
  if (field_alias2name == nullptr) field_alias2name = std::make_shared<std::unordered_map<string, string>>();

  BinderContext binder_context;

  // collect tables in `from` statement
  vector<Table *>                tables;
  unordered_map<string, Table *> table_map;

  // 首先将 loaded_relation_names 中的表名添加到 table_map 中
  // 由于处理子查询是递归进行的，只会由外向内传，所以内层的 sub select 
  // 会额外拥有外层扫到的 table，而外层不会。
  for (auto &rel_name : *loaded_relation_names) {
    // TODO(Soulter): 这里待优化，也就是缓存一下 table 实例的指针。 UPDATE：不能缓存。
    Table *table = db->find_table(rel_name.c_str());
    if (nullptr == table) {
      LOG_WARN("no such table. db=%s, table_name=%s", db->name(), rel_name.c_str());
      return RC::SCHEMA_TABLE_NOT_EXIST;
    }
    table->set_is_outer_table(true); // 对于当前查询来说，这是来自外层的表。做个标记。
    table_map.insert({rel_name, table});
    LOG_DEBUG("add table from name2alias extraly(sub-query): %s", rel_name.c_str());
  }

  // 然后才是处理 select 语句中的 from 语句
  std::vector<std::string> tables_alias;
  for (size_t i = 0; i < select_sql.relations.size(); i++) {
    const char *table_name = select_sql.relations[i].name.c_str();
    // const char *table_alias = select_sql.relations[i].alias.c_str();
    if (nullptr == table_name) {
      LOG_WARN("invalid argument. relation name is null. index=%d", i);
      return RC::INVALID_ARGUMENT;
    }

    Table *table = db->find_table(table_name);
    // table->set_table_alias(table_alias);
    if (nullptr == table) {
      LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
      return RC::SCHEMA_TABLE_NOT_EXIST;
    }

    binder_context.add_table(table);
    tables.push_back(table);
    tables_alias.push_back(select_sql.relations[i].alias);
    table_map.insert({table_name, table});

    // 检查 alias 重复
    for (size_t j = i + 1; j < select_sql.relations.size(); j++) {
      if (select_sql.relations[i].alias.empty() || select_sql.relations[j].alias.empty()) continue;
      if (select_sql.relations[i].alias == select_sql.relations[j].alias) {
        LOG_WARN("duplicate alias: %s", select_sql.relations[i].alias.c_str());
        return RC::INVALID_ARGUMENT;
      }
    }

    if (!select_sql.relations[i].alias.empty()) {
      // 非空才存，防止重复存到空的 alias 导致 duplicate error。
      // 在alias2name中检查 alias 是否重复
      // UPDATE: 不需要子表和外表的 alias 重复检查，因为外表的 alias 可以被子表的 alias 覆盖。
      // if (alias2name->find(select_sql.relations[i].alias) != alias2name->end()) {
      //   LOG_WARN("duplicate alias found in from statement: %s", select_sql.relations[i].alias.c_str());
      //   return RC::INVALID_ARGUMENT;
      // }

      // 一切没问题之后，
      // 备用表名和别名的映射
      name2alias->insert({table_name, select_sql.relations[i].alias});
      alias2name->insert({select_sql.relations[i].alias, table_name});
    }
    loaded_relation_names->push_back(table_name);
  }

  // 将 expressions（要 select 的表达式）中带有别名的表名替换为真实的表名
  for (auto &expression : select_sql.expressions) {
    RC rc = convert_alias_to_name(expression.get(), alias2name, nullptr);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to convert alias to name");
      return rc;
    }

    // 如果是字段表达式并且有别名，将字段名和别名的映射存起来
    if (expression->type() == ExprType::UNBOUND_FIELD) {
      UnboundFieldExpr *ub_field_expr = static_cast<UnboundFieldExpr *>(expression.get());
      if (!ub_field_expr->alias_std_string().empty()) {
        field_alias2name->insert({ub_field_expr->alias_std_string(), ub_field_expr->field_name()});
      }
    }

    // 如果是 StarExpr，检查是否有别名，如果有报错
    if (expression->type() == ExprType::STAR) {
      StarExpr *star_expr = static_cast<StarExpr *>(expression.get());
      if (!star_expr->alias_std_string().empty()) {
        LOG_WARN("alias found in star expression");
        return RC::INVALID_ARGUMENT;
      }
    }
  }
  
  // 将 conditions 中 **所有** 带有别名的表名替换为真实的表名
  // 将 conditions 中 **所有** 使用别名 field 的表达式替换为真实的字段名
  for (auto &condition : select_sql.conditions) {
    if (condition.left_expr != nullptr) {
      RC rc = convert_alias_to_name(condition.left_expr.get(), alias2name, field_alias2name);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to convert alias to name");
        return rc;
      }
    }
    if (condition.right_expr != nullptr) {
      RC rc = convert_alias_to_name(condition.right_expr.get(), alias2name, field_alias2name);
      if (rc != RC::SUCCESS) {
        LOG_WARN("failed to convert alias to name");
        return rc;
      }
    }
  }

  // 如果有聚合表达式，检查非聚合表达式是否在 group by 语句中
  // 目前只能判断简单的情况，无法判断嵌套的聚合表达式
  bool has_aggregation = false;
  for (unique_ptr<Expression> &expression : select_sql.expressions) {
    if (expression->type() == ExprType::UNBOUND_AGGREGATION) {
      has_aggregation = true;
      break;
    }
  }
  if (has_aggregation) {
    for (unique_ptr<Expression> &select_expr : select_sql.expressions) {
      if (select_expr->type() == ExprType::UNBOUND_AGGREGATION) {
        continue;
      }

      if (select_expr->type() == ExprType::ARITHMETIC) {
        ArithmeticExpr *arith_expr = static_cast<ArithmeticExpr *>(select_expr.get());
        if (arith_expr->left() != nullptr && arith_expr->left()->type() == ExprType::UNBOUND_AGGREGATION && arith_expr->right() != nullptr && arith_expr->right()->type() == ExprType::UNBOUND_AGGREGATION) {
          continue;
        }
      }


      bool found = false;
      for (unique_ptr<Expression> &group_by_expr : select_sql.group_by) {
        if (select_expr->equal(*group_by_expr)) {
          found = true;
          break;
        }
      }
      if (!found) {
        LOG_WARN("non-aggregation expression found in select statement but not in group by statement");
        return RC::INVALID_ARGUMENT;
      }
    }
  }

  // 接下来是绑定表达式，指的是将表达式中的字段和 table 关联起来

  // collect query fields in `select` statement
  vector<unique_ptr<Expression>> bound_expressions;
  ExpressionBinder               expression_binder(binder_context);

  // 遍历 select 语句中的要查询的字段的表达式, 绑定表达式
  // (hint: condition 的表达式绑定在 filterstmt::create 中)
  for (unique_ptr<Expression> &expression : select_sql.expressions) {
    RC rc = expression_binder.bind_expression(expression, bound_expressions);
    if (OB_FAIL(rc)) {
      LOG_INFO("bind expression failed. rc=%s", strrc(rc));
      return rc;
    }
  }

  // 遍历 group by 语句中的表达式, 绑定表达式
  vector<unique_ptr<Expression>> group_by_expressions;
  for (unique_ptr<Expression> &expression : select_sql.group_by) {
    RC rc = expression_binder.bind_expression(expression, group_by_expressions);
    if (OB_FAIL(rc)) {
      LOG_INFO("bind expression failed. rc=%s", strrc(rc));
      return rc;
    }
  }

  // 遍历 order by 语句中的表达式, 绑定表达式
  vector<unique_ptr<Expression>> order_by_exprs;
  vector<bool>                   order_by_descs;
  for (OrderBySqlNode &order_by : select_sql.order_by) {
    RC rc = expression_binder.bind_expression(order_by.expression, order_by_exprs);
    if (OB_FAIL(rc)) {
      LOG_INFO("bind expression failed. rc=%s", strrc(rc));
      return rc;
    }
    order_by_descs.push_back(order_by.is_desc);
  }

  // 子查询，遍历 conditions 中的表达式，（递归）创建对应的 stmt。
  // 这个 for 会将所有的子查询的 stmt 都创建好，放到 SubqueryExpr 中
  for (auto &condition : select_sql.conditions) {
    // exists/not exists 可能会使得 left_expr 为空
    if (condition.left_expr != nullptr && condition.left_expr->type() == ExprType::SUB_QUERY) {
      SubqueryExpr *subquery_expr = static_cast<SubqueryExpr *>(condition.left_expr.get());
      Stmt         *stmt          = nullptr;
      RC            rc            = SelectStmt::create(
        db, 
        subquery_expr->sub_query_sn()->selection, 
        stmt, 
        name2alias, 
        alias2name, 
        loaded_relation_names,
        field_alias2name
      );
      if (rc != RC::SUCCESS) {
        LOG_WARN("cannot construct subquery stmt");
        return rc;
      }
      // 检查子查询的合法性：子查询的查询的属性只能有一个
      RC rc_ = check_sub_select_legal(db, subquery_expr->sub_query_sn());
      if (rc_ != RC::SUCCESS) {
        return rc_;
      }
      subquery_expr->set_stmt(unique_ptr<SelectStmt>(static_cast<SelectStmt *>(stmt)));
    }
    if (condition.right_expr != nullptr && condition.right_expr->type() == ExprType::SUB_QUERY) {
      SubqueryExpr *subquery_expr = static_cast<SubqueryExpr *>(condition.right_expr.get());
      Stmt         *stmt          = nullptr;
      RC            rc            = SelectStmt::create(
        db,
        subquery_expr->sub_query_sn()->selection, 
        stmt, 
        name2alias, 
        alias2name, 
        loaded_relation_names,
        field_alias2name
      );
      if (rc != RC::SUCCESS) {
        LOG_WARN("cannot construct subquery stmt");
        return rc;
      }
      // 检查子查询的合法性：子查询的查询的属性只能有一个
      RC rc_ = check_sub_select_legal(db, subquery_expr->sub_query_sn());
      if (rc_ != RC::SUCCESS) {
        return rc_;
      }
      subquery_expr->set_stmt(unique_ptr<SelectStmt>(static_cast<SelectStmt *>(stmt)));
    }
  }

  Table *default_table = nullptr;
  if (tables.size() == 1) {
    default_table = tables[0];
  }

  // create filter statement in `where` statement
  FilterStmt *filter_stmt = nullptr;
  RC          rc          = FilterStmt::create(db, default_table, &table_map, select_sql.conditions, filter_stmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("cannot construct filter stmt");
    return rc;
  }


  // create filter statement in `having` statement
  FilterStmt *filter_stmt_having = nullptr;
  if (!select_sql.havings.empty()) {
    RC rc = FilterStmt::create(db, default_table, &table_map, select_sql.havings, filter_stmt_having);
    if (rc != RC::SUCCESS) {
      LOG_WARN("cannot construct filter stmt");
      return rc;
    }
  }
  
  // everything alright
  auto *select_stmt = new SelectStmt();

  select_stmt->tables_.swap(tables);
  select_stmt->table_alias_.swap(tables_alias);
  select_stmt->query_expressions_.swap(bound_expressions);
  select_stmt->filter_stmt_ = filter_stmt;
  select_stmt->group_by_.swap(group_by_expressions);
  select_stmt->filter_stmt_having_ = filter_stmt_having;
  select_stmt->order_by_exprs_.swap(order_by_exprs);
  select_stmt->order_by_descs_.swap(order_by_descs);
  select_stmt->limit_ = select_sql.limit;
  stmt = select_stmt;
  return RC::SUCCESS;
}
