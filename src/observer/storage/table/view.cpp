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

#include "storage/record/record_manager.h"
#include "storage/record/phy_op_record_scanner.h"
#include <storage/table/view.h>
#include <common/types.h>


void View::init_table_meta(const vector<FieldMeta> &fields) {
    std::vector<AttrInfoSqlNode> attr_infos;

    size_t idx = 0;
    for (const auto & field : fields) {
        AttrInfoSqlNode attr_info;
        attr_info.nullable = field.nullable();
        if (attrs_name_.empty()) { 
            attr_info.name = field.name();
        } else attr_info.name = attrs_name_[idx];
        attr_info.type = field.type();
        if (attr_info.type == AttrType::VECTORS) {
            attr_info.arr_len = field.vector_dim();
        } else if (attr_info.type == AttrType::CHARS) {
            attr_info.arr_len = field.len();
        } else {
            attr_info.arr_len = 1;
        }
        attr_infos.push_back(attr_info);

        // field name 不可能重复
        if (attrs_name_.empty()) { 
            field_base_table_name[field.name()] = field.table_name_;
        } else {
            attr_name_2_base_table_field_name[attrs_name_[idx]] = field.name();
            field_base_table_name[attrs_name_[idx]] = field.table_name_;
        }

        idx += 1;
    }
    table_meta_.init(view_id_, view_name_.c_str(), nullptr, attr_infos, StorageFormat::ROW_FORMAT);
}

RC View::get_record_scanner(RecordPhysicalOperatorScanner &scanner, Trx *trx, ReadWriteMode mode) {
    RC rc = RC::SUCCESS;
    scanner.set_oper(std::move(operator_));
    rc = scanner.open_oper(trx);
    if (rc != RC::SUCCESS) {
        LOG_WARN("View: failed to open operator: %s", strrc(rc));
        return rc;
    }
    return rc;
} 