/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.gsi.GsiManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import org.mockito.MockedStatic;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LogicalShowCreateTableTest {
    protected static final String SCHEMA_NAME = "test_show_create_table_name";
    protected static final String TABLE_NAME = "test_show_create_table_name";
    protected static final String SHOW_CREATE_TABLE = "show create table " + TABLE_NAME;

    protected static void mockMetaSystem(String schemaName,
                                         String tableName,
                                         MockedStatic<OptimizerContext> mockOptimizerContextStatic,
                                         MockedStatic<ExecutorContext> mockExecutorContextStatic) {
        final TableMeta mockTableMeta = mock(TableMeta.class);
        lenient().when(mockTableMeta.getEngine()).thenReturn(Engine.INNODB);

        final SchemaManager schemaManager = mock(SchemaManager.class);
        lenient().when(schemaManager.getTableWithNull(contains(tableName))).thenReturn(mockTableMeta);

        final PartitionInfo partitionInfo = mock(PartitionInfo.class);
        lenient().when(partitionInfo.showCreateTablePartitionDefInfo(anyBoolean())).thenReturn("");

        final PartitionInfoManager partitionInfoManager = mock(PartitionInfoManager.class);
        lenient().when(partitionInfoManager.getPartitionInfo(contains(tableName))).thenReturn(partitionInfo);

        final TddlRuleManager tddlRuleManager = mock(TddlRuleManager.class);
        lenient().when(tddlRuleManager.getPartitionInfoManager()).thenReturn(partitionInfoManager);

        final TableGroupInfoManager mockTableGroupInfoManager = mock(TableGroupInfoManager.class);
        lenient().when(mockTableGroupInfoManager.getTableGroupConfigById(any())).thenReturn(null);

        final OptimizerContext mockOptimizerContext = mock(OptimizerContext.class);
        when(mockOptimizerContext.getLatestSchemaManager()).thenReturn(schemaManager);
        when(mockOptimizerContext.getRuleManager()).thenReturn(tddlRuleManager);
        lenient().when(mockOptimizerContext.getTableGroupInfoManager()).thenReturn(mockTableGroupInfoManager);

        mockOptimizerContextStatic
            .when(() -> OptimizerContext.getContext(contains(schemaName)))
            .thenReturn(mockOptimizerContext);

        final GsiManager gsiManager = mock(GsiManager.class);
        when(gsiManager.getGsiTableAndIndexMeta(contains(schemaName), contains(tableName), any()))
            .thenReturn(GsiMetaManager.GsiMetaBean.empty());

        final ExecutorContext mockExecutorContext = mock(ExecutorContext.class);
        when(mockExecutorContext.getGsiManager()).thenReturn(gsiManager);

        mockExecutorContextStatic
            .when(() -> ExecutorContext.getContext(contains(schemaName)))
            .thenReturn(mockExecutorContext);
    }

    protected static CursorMeta createCursorMeta() {
        final ArrayResultCursor tmpCursor = new ArrayResultCursor("Create Table");
        tmpCursor.addColumn("Table", DataTypes.StringType, false);
        tmpCursor.addColumn("Create Table", DataTypes.StringType, false);
        tmpCursor.initMeta();
        return tmpCursor.getCursorMeta();
    }
}
