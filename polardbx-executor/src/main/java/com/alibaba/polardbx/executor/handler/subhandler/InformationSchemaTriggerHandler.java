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

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.view.InformationSchemaTriggers;
import com.alibaba.polardbx.optimizer.view.VirtualView;

/**
 * @author chenzilin
 */
public class InformationSchemaTriggerHandler extends BaseVirtualViewSubClassHandler {

    public InformationSchemaTriggerHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {

        if (!executionContext.getParamManager()
            .getBoolean(ConnectionParams.ENABLE_TRIGGER_DIRECT_INFORMATION_SCHEMA_QUERY)) {
            return cursor;
        }

        String querySchemaName = executionContext.getSchemaName();
        if ("polardbx".equalsIgnoreCase(querySchemaName)) {
            String schemaSql =
                "select SCHEMA_NAME from information_schema.schemata where schema_name not in ('information_schema', '__cdc__') limit 1;";
            ExecutionContext newEc = executionContext.copy();
            newEc.newStatement();
            ExecutionPlan ep = Planner.getInstance().plan(schemaSql, newEc);
            Cursor schemaCursor = ExecutorHelper.execute(ep.getPlan(), newEc);

            try {
                Row row = null;
                while ((row = schemaCursor.next()) != null) {
                    querySchemaName = row.getString(0);
                }
            } catch (Throwable t) {
                // ignore
            }
        }

        String sql = "/*+TDDL:NODE(0)*/\n" +
            "select TRIGGER_CATALOG,TRIGGER_SCHEMA,TRIGGER_NAME,EVENT_MANIPULATION,EVENT_OBJECT_CATALOG,EVENT_OBJECT_SCHEMA,EVENT_OBJECT_TABLE,ACTION_ORDER,ACTION_CONDITION,ACTION_STATEMENT,ACTION_ORIENTATION,ACTION_TIMING,ACTION_REFERENCE_OLD_TABLE,ACTION_REFERENCE_NEW_TABLE,ACTION_REFERENCE_OLD_ROW,ACTION_REFERENCE_NEW_ROW,CREATED,SQL_MODE,DEFINER,CHARACTER_SET_CLIENT,COLLATION_CONNECTION,DATABASE_COLLATION from information_schema.triggers;";
        ExecutionContext newExecutionContext = executionContext.copy();
        newExecutionContext.newStatement();
        ExecutionPlan executionPlan = Planner.getInstance().plan(sql, newExecutionContext);
        Cursor c = ExecutorHelper.execute(executionPlan.getPlan(), newExecutionContext);
        Row row = null;
        while ((row = c.next()) != null) {
//            "TRIGGER_CATALOG"
//            "TRIGGER_SCHEMA"
//            "TRIGGER_NAME"
//            "EVENT_MANIPULATION"
//            "EVENT_OBJECT_CATALOG"
//            "EVENT_OBJECT_SCHEMA"
//            "EVENT_OBJECT_TABLE"
//            "ACTION_ORDER"
//            "ACTION_CONDITION"
//            "ACTION_STATEMENT"
//            "ACTION_ORIENTATION"
//            "ACTION_TIMING"
//            "ACTION_REFERENCE_OLD_TABLE"
//            "ACTION_REFERENCE_NEW_TABLE"
//            "ACTION_REFERENCE_OLD_ROW"
//            "ACTION_REFERENCE_NEW_ROW"
//            "CREATED"
//            "SQL_MODE"
//            "DEFINER"
//            "CHARACTER_SET_CLIENT"
//            "COLLATION_CONNECTION"
//            "DATABASE_COLLATION"
            String phySchemaName = row.getString(1);
            String schemaName = phySchemaName;
            if (phySchemaName.equalsIgnoreCase("sys")) {
                continue;
            }
            int idx1 = phySchemaName.lastIndexOf("_p00000");
            if (idx1 != -1) {
                schemaName = phySchemaName.substring(0, idx1);
            } else {
                int idx2 = phySchemaName.lastIndexOf("_single");
                if (idx2 != -1) {
                    schemaName = phySchemaName.substring(0, idx2);
                }
            }

            cursor.addRow(new Object[] {
                row.getObject(0),
                schemaName,
                row.getObject(2),
                row.getObject(3),
                row.getObject(4),
                row.getObject(5),
                row.getObject(6),
                row.getObject(7),
                row.getObject(8),
                row.getObject(9),
                row.getObject(10),
                row.getObject(11),
                row.getObject(12),
                row.getObject(13),
                row.getObject(14),
                row.getObject(15),
                row.getObject(16),
                row.getObject(17),
                row.getObject(18),
                row.getObject(19),
                row.getObject(20),
                row.getObject(21),
            });
        }
        return cursor;
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaTriggers;
    }

}

