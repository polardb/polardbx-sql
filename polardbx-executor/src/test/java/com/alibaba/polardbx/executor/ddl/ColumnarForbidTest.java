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

package com.alibaba.polardbx.executor.ddl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.executor.ddl.job.builder.tablegroup.AlterTableGroupBaseBuilder;
import com.alibaba.polardbx.executor.ddl.job.builder.tablegroup.AlterTableGroupModifyPartitionBuilder;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TruncateTableValidateTask;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.AbstractDdlTask;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupBasePreparedData;
import org.apache.calcite.sql.SqlKind;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.MockedStatic;

import java.lang.reflect.Field;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class ColumnarForbidTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void ForbidTruncateCciTable() throws Exception {
        TruncateTableValidateTask truncateTableValidateTask = mock(TruncateTableValidateTask.class);
        ExecutionContext ec = mock(ExecutionContext.class);

        try (MockedStatic<TableValidator> staticTableValidator = mockStatic(TableValidator.class)) {
            staticTableValidator.when(
                () -> TableValidator.validateTableExistence(any(String.class), any(String.class),
                    any(ExecutionContext.class))).thenAnswer(invocation -> null);
            staticTableValidator.when(
                () -> TableValidator.validateTableNotReferenceFk(any(String.class), any(String.class),
                    any(ExecutionContext.class))).thenAnswer(invocation -> null);
            staticTableValidator.when(
                    () -> TableValidator.validateTableWithCCI("schema", "table", ec, SqlKind.TRUNCATE_TABLE))
                .thenCallRealMethod();

            try (MockedStatic<OptimizerContext> staticOptimizerContext = mockStatic(OptimizerContext.class)) {
                OptimizerContext optimizerContext = mock(OptimizerContext.class);
                SchemaManager schemaManager = mock(SchemaManager.class);
                TableMeta tableMeta = mock(TableMeta.class);
                ParamManager paramManager = mock(ParamManager.class);
                staticOptimizerContext.when(() -> OptimizerContext.getContext(any())).thenReturn(optimizerContext);
                when(optimizerContext.getLatestSchemaManager()).thenReturn(schemaManager);
                when(schemaManager.getTableWithNull(any())).thenReturn(tableMeta);
                when(ec.getParamManager()).thenReturn(paramManager);
                when(paramManager.getBoolean(any())).thenReturn(true);
                when(tableMeta.withCci()).thenReturn(true);

                Field field1 = AbstractDdlTask.class.getDeclaredField("schemaName");
                field1.setAccessible(true);
                field1.set(truncateTableValidateTask, "schema");
                Field field2 = TruncateTableValidateTask.class.getDeclaredField("logicalTableName");
                field2.setAccessible(true);
                field2.set(truncateTableValidateTask, "table");

                thrown.expect(TddlRuntimeException.class);
                thrown.expectMessage("Do not support current ddl");
                doCallRealMethod().when(truncateTableValidateTask).executeImpl(ec);
                truncateTableValidateTask.executeImpl(ec);
            }
        }

    }

    @Test
    public void ForbidTruncateCciTable1() {
        TruncateTableValidateTask truncateTableValidateTask = mock(TruncateTableValidateTask.class);
        ExecutionContext ec = mock(ExecutionContext.class);

        try (MockedStatic<TableValidator> staticTableValidator = mockStatic(TableValidator.class)) {
            staticTableValidator.when(
                () -> TableValidator.validateTableExistence(any(String.class), any(String.class),
                    any(ExecutionContext.class))).thenAnswer(invocation -> null);
            staticTableValidator.when(
                () -> TableValidator.validateTableNotReferenceFk(any(String.class), any(String.class),
                    any(ExecutionContext.class))).thenAnswer(invocation -> null);
            staticTableValidator.when(
                    () -> TableValidator.validateTableWithCCI("schema", "table", ec, SqlKind.TRUNCATE_TABLE))
                .thenAnswer(invocation -> null);

            doCallRealMethod().when(truncateTableValidateTask).executeImpl(ec);
            truncateTableValidateTask.executeImpl(ec);
        }
    }

    @Test(expected = TddlRuntimeException.class)
    public void validateTableWithCciThrowTest() {
        ParamManager paramManager = mock(ParamManager.class);
        ExecutionContext executionContext = mock(ExecutionContext.class);
        when(executionContext.getParamManager()).thenReturn(paramManager);
        when(paramManager.getBoolean(any())).thenReturn(true);
        ComplexTaskMetaManager.ComplexTaskType complexTaskType = ComplexTaskMetaManager.ComplexTaskType.ADD_PARTITION;
        TableValidator.validateTableWithCCI(executionContext, complexTaskType);
        complexTaskType = ComplexTaskMetaManager.ComplexTaskType.DROP_PARTITION;
        TableValidator.validateTableWithCCI(executionContext, complexTaskType);
        complexTaskType = ComplexTaskMetaManager.ComplexTaskType.SPLIT_PARTITION;
        TableValidator.validateTableWithCCI(executionContext, complexTaskType);
    }

    @Test
    public void validateTableWithCciTest() {
        ParamManager paramManager = mock(ParamManager.class);
        ExecutionContext executionContext = mock(ExecutionContext.class);
        when(executionContext.getParamManager()).thenReturn(paramManager);
        when(paramManager.getBoolean(any())).thenReturn(true);
        ComplexTaskMetaManager.ComplexTaskType complexTaskType = ComplexTaskMetaManager.ComplexTaskType.MOVE_PARTITION;
        TableValidator.validateTableWithCCI(executionContext, complexTaskType);
    }

    @Test(expected = TddlRuntimeException.class)
    public void validateTableWithCciThrowTest1() {
        AlterTableGroupBaseBuilder builder = mock(AlterTableGroupBaseBuilder.class);
        AlterTableGroupBasePreparedData preparedData = mock(AlterTableGroupBasePreparedData.class);
        ExecutionContext executionContext = mock(ExecutionContext.class);
        SchemaManager schemaManager = mock(SchemaManager.class);
        TableMeta tableMeta = mock(TableMeta.class);
        when(builder.getExecutionContext()).thenReturn(executionContext);
        when(builder.getPreparedData()).thenReturn(preparedData);
        when(preparedData.getSchemaName()).thenReturn("schema");
        when(executionContext.getSchemaManager(any())).thenReturn(schemaManager);
        when(schemaManager.getTable(any())).thenReturn(tableMeta);
        when(tableMeta.withCci()).thenReturn(true);
        ParamManager paramManager = mock(ParamManager.class);
        when(executionContext.getParamManager()).thenReturn(paramManager);
        when(paramManager.getBoolean(any())).thenReturn(true);

        doCallRealMethod().when(builder).createAlterTableGroupItemPreparedData(any(), any());
        when(preparedData.getTaskType()).thenReturn(ComplexTaskMetaManager.ComplexTaskType.ADD_PARTITION);
        builder.createAlterTableGroupItemPreparedData("tableName", null);
        when(preparedData.getTaskType()).thenReturn(ComplexTaskMetaManager.ComplexTaskType.DROP_PARTITION);
        builder.createAlterTableGroupItemPreparedData("tableName", null);
        when(preparedData.getTaskType()).thenReturn(ComplexTaskMetaManager.ComplexTaskType.SPLIT_PARTITION);
        builder.createAlterTableGroupItemPreparedData("tableName", null);
    }

    @Test(expected = NullPointerException.class)
    public void validateTableWithCciTest1() {
        AlterTableGroupBaseBuilder builder = mock(AlterTableGroupBaseBuilder.class);
        AlterTableGroupBasePreparedData preparedData = mock(AlterTableGroupBasePreparedData.class);
        ExecutionContext executionContext = mock(ExecutionContext.class);
        SchemaManager schemaManager = mock(SchemaManager.class);
        TableMeta tableMeta = mock(TableMeta.class);
        when(builder.getExecutionContext()).thenReturn(executionContext);
        when(builder.getPreparedData()).thenReturn(preparedData);
        when(preparedData.getSchemaName()).thenReturn("schema");
        when(executionContext.getSchemaManager(any())).thenReturn(schemaManager);
        when(schemaManager.getTable(any())).thenReturn(tableMeta);
        when(tableMeta.withCci()).thenReturn(true);
        ParamManager paramManager = mock(ParamManager.class);
        when(executionContext.getParamManager()).thenReturn(paramManager);
        when(paramManager.getBoolean(any())).thenReturn(true);

        doCallRealMethod().when(builder).createAlterTableGroupItemPreparedData(any(), any());
        when(preparedData.getTaskType()).thenReturn(ComplexTaskMetaManager.ComplexTaskType.MOVE_PARTITION);
        builder.createAlterTableGroupItemPreparedData("tableName", null);
    }

    @Test(expected = TddlRuntimeException.class)
    public void validateTableWithCciTest2() {
        AlterTableGroupModifyPartitionBuilder builder = mock(AlterTableGroupModifyPartitionBuilder.class);
        AlterTableGroupBasePreparedData preparedData = mock(AlterTableGroupBasePreparedData.class);
        ExecutionContext executionContext = mock(ExecutionContext.class);
        SchemaManager schemaManager = mock(SchemaManager.class);
        TableMeta tableMeta = mock(TableMeta.class);
        when(builder.getExecutionContext()).thenReturn(executionContext);
        when(builder.getPreparedData()).thenReturn(preparedData);
        when(preparedData.getSchemaName()).thenReturn("schema");
        when(executionContext.getSchemaManager(any())).thenReturn(schemaManager);
        when(schemaManager.getTable(any())).thenReturn(tableMeta);
        when(tableMeta.withCci()).thenReturn(true);
        ParamManager paramManager = mock(ParamManager.class);
        when(executionContext.getParamManager()).thenReturn(paramManager);
        when(paramManager.getBoolean(any())).thenReturn(true);

        doCallRealMethod().when(builder).createAlterTableGroupItemPreparedData(any(), any());
        when(preparedData.getTaskType()).thenReturn(ComplexTaskMetaManager.ComplexTaskType.REORGANIZE_PARTITION);
        builder.createAlterTableGroupItemPreparedData("tableName", null);
    }
}
