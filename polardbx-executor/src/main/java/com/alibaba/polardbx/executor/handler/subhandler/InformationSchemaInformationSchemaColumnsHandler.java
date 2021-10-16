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

package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaInformationSchemaColumns;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import com.alibaba.polardbx.optimizer.view.VirtualViewType;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.List;

/**
 * @author shengyu
 */
public class InformationSchemaInformationSchemaColumnsHandler extends BaseVirtualViewSubClassHandler {

    public InformationSchemaInformationSchemaColumnsHandler(
        VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaInformationSchemaColumns;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        InformationSchemaInformationSchemaColumns informationSchemaInformationSchemaColumns
            = (InformationSchemaInformationSchemaColumns) virtualView;
        for (VirtualViewType virtualViewType : VirtualViewType.values()) {
            VirtualView vv = VirtualView.create(informationSchemaInformationSchemaColumns.getCluster(),
                virtualViewType);
            List<RelDataTypeField> fieldList = vv.getRowType().getFieldList();
            for (int i = 0; i < fieldList.size(); i++) {
                RelDataTypeField field = fieldList.get(i);
                cursor.addRow(new Object[] {
                    "def",
                    "information_schema",
                    virtualViewType,
                    field.getName(),
                    i + 1,
                    null,
                    field.getType().isNullable(),
                    field.getType().getSqlTypeName(),
                    100,
                    100,
                    null,
                    null,
                    null,
                    "utf8",
                    "utf8_general_ci",
                    field.getType().getSqlTypeName(),
                    "",
                    "",
                    "select",
                    "",
                    null,
                    // GENERATION_EXPRESSION mysql 5.6 do not contain this column, we use default null to resolve
                });
            }
        }
        return cursor;
    }
}
