/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.optimizer.config.schema;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.MysqlSchemaViewManager;
import com.alibaba.polardbx.optimizer.view.SystemTableView;
import com.alibaba.polardbx.optimizer.view.VirtualViewType;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.ViewTable;

import java.util.List;

/**
 * @author dylan
 */
public class MysqlSchema extends AbsSchema {

    public static final String NAME = "MYSQL";

    private static final MysqlSchema INSTANCE = new MysqlSchema();

    private MysqlSchema() {

    }

    public static MysqlSchema getInstance() {
        return INSTANCE;
    }

    @Override
    public Table getTable(String tableName) {
        SystemTableView.Row row = MysqlSchemaViewManager.getInstance().select(tableName);
        if (row != null) {
            String viewDefinition = row.getViewDefinition();
            List<String> columnList = row.getColumnList();
            RelProtoDataType relProtoDataType;
            if (row.isVirtual()) {
                VirtualViewType virtualViewType = row.getVirtualViewType();
                relProtoDataType =
                    new TddlCalciteSchema.VirtualViewProtoDataType(virtualViewType, new ExecutionContext());
            } else {
                relProtoDataType =
                    new TddlCalciteSchema.ViewProtoDataType(columnList, viewDefinition, new ExecutionContext());
            }
            Table table = new ViewTable(null, relProtoDataType, viewDefinition, ImmutableList.<String>of(), null);
            return table;
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_TABLE, tableName);
        }
    }
}


