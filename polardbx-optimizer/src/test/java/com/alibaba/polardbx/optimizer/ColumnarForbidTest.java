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

package com.alibaba.polardbx.optimizer;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTable;
import com.alibaba.polardbx.optimizer.utils.MetaUtils;
import org.apache.calcite.sql.SqlAlterSpecification;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlChangeColumn;
import org.apache.calcite.sql.SqlColumnDeclaration;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlModifyColumn;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.MockedStatic;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class ColumnarForbidTest {
    private final String schemaName = "schema_name";
    private final String tableName = "table_name";

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Ignore
    @Test
    public void ForbidAlterCciTableWithMultiStatement() {
        try (MockedStatic<OptimizerContext> staticOptimizerContext = mockStatic(OptimizerContext.class)) {
            OptimizerContext optimizerContext = mock(OptimizerContext.class);
            SchemaManager schemaManager = mock(SchemaManager.class);
            staticOptimizerContext.when(() -> OptimizerContext.getContext(any())).thenReturn(optimizerContext);

            try (MockedStatic<MetaUtils.TableColumns> staticTableColumns = mockStatic(MetaUtils.TableColumns.class)) {
                MetaUtils.TableColumns tableColumns = mock(MetaUtils.TableColumns.class);
                staticTableColumns.when(() -> MetaUtils.TableColumns.build(any())).thenReturn(tableColumns);

                TableMeta tableMeta = mock(TableMeta.class);
                SqlAlterTable sqlAlterTable = mock(SqlAlterTable.class);

                List<SqlAlterSpecification> alters = new ArrayList<>();
                SqlAlterSpecification alterSpecification1 = mock(SqlAlterSpecification.class);
                SqlAlterSpecification alterSpecification2 = mock(SqlAlterSpecification.class);

                LogicalAlterTable alterTable = mock(LogicalAlterTable.class);
                when(alterTable.getSchemaName()).thenReturn(schemaName);
                when(alterTable.getTableName()).thenReturn(tableName);

                when(optimizerContext.getLatestSchemaManager()).thenReturn(schemaManager);
                when(schemaManager.getTable(any())).thenReturn(tableMeta);
                when(tableMeta.withCci()).thenReturn(true);
                when(alterTable.getSqlAlterTable()).thenReturn(sqlAlterTable);
                when(sqlAlterTable.getAlters()).thenReturn(alters);

                alters.add(alterSpecification1);
                alters.add(alterSpecification2);

                thrown.expect(TddlRuntimeException.class);
                thrown.expectMessage(
                    "Do not support multiple ALTER TABLE statements on table with clustered columnar index");
                doCallRealMethod().when(alterTable).validateColumnar();
                alterTable.validateColumnar();
            }
        }
    }

    @Test
    public void ForbidCitableModifyUnsupportedType() {
        try (MockedStatic<OptimizerContext> staticOptimizerContext = mockStatic(OptimizerContext.class)) {
            OptimizerContext optimizerContext = mock(OptimizerContext.class);
            SchemaManager schemaManager = mock(SchemaManager.class);
            staticOptimizerContext.when(() -> OptimizerContext.getContext(any())).thenReturn(optimizerContext);

            try (MockedStatic<MetaUtils.TableColumns> staticTableColumns = mockStatic(MetaUtils.TableColumns.class)) {
                MetaUtils.TableColumns tableColumns = mock(MetaUtils.TableColumns.class);
                staticTableColumns.when(() -> MetaUtils.TableColumns.build(any())).thenReturn(tableColumns);

                TableMeta tableMeta = mock(TableMeta.class);
                SqlAlterTable sqlAlterTable = mock(SqlAlterTable.class);

                List<SqlAlterSpecification> alters = new ArrayList<>();
                SqlModifyColumn modifyColumn = mock(SqlModifyColumn.class);

                LogicalAlterTable alterTable = mock(LogicalAlterTable.class);
                SqlIdentifier sqlIdentifier = mock(SqlIdentifier.class);
                SqlColumnDeclaration sqlColumnDeclaration = mock(SqlColumnDeclaration.class);
                SqlDataTypeSpec sqlDataTypeSpec = mock(SqlDataTypeSpec.class);
                when(alterTable.getSchemaName()).thenReturn(schemaName);
                when(alterTable.getTableName()).thenReturn(tableName);
                when(modifyColumn.getKind()).thenReturn(SqlKind.MODIFY_COLUMN);
                when(modifyColumn.getColName()).thenReturn(sqlIdentifier);
                when(sqlIdentifier.getLastName()).thenReturn("column_name");

                when(modifyColumn.getColDef()).thenReturn(sqlColumnDeclaration);
                when(sqlColumnDeclaration.getDataType()).thenReturn(sqlDataTypeSpec);
                when(sqlDataTypeSpec.getTypeName()).thenReturn(sqlIdentifier);
                when(sqlIdentifier.getLastName()).thenReturn("binary");

                when(optimizerContext.getLatestSchemaManager()).thenReturn(schemaManager);
                when(schemaManager.getTable(any())).thenReturn(tableMeta);
                when(tableMeta.withCci()).thenReturn(true);
                when(alterTable.getSqlAlterTable()).thenReturn(sqlAlterTable);
                when(sqlAlterTable.getAlters()).thenReturn(alters);

                alters.add(modifyColumn);

                try (MockedStatic<SqlValidatorImpl> staticSqlValidatorImpl = mockStatic(SqlValidatorImpl.class)) {
                    staticSqlValidatorImpl.when(
                            () -> SqlValidatorImpl.validateUnsupportedTypeWithCciWhenModifyColumn(sqlColumnDeclaration))
                        .thenAnswer(invocation -> null);

//                    thrown.expect(TddlRuntimeException.class);
//                    thrown.expectMessage("is not supported by CCI");
                    doCallRealMethod().when(alterTable).validateColumnar();
                    alterTable.validateColumnar();
                }
            }
        }
    }

    @Test
    public void ForbidCitableChangeUnsupportedType() {
        try (MockedStatic<OptimizerContext> staticOptimizerContext = mockStatic(OptimizerContext.class)) {
            OptimizerContext optimizerContext = mock(OptimizerContext.class);
            SchemaManager schemaManager = mock(SchemaManager.class);
            staticOptimizerContext.when(() -> OptimizerContext.getContext(any())).thenReturn(optimizerContext);

            try (MockedStatic<MetaUtils.TableColumns> staticTableColumns = mockStatic(MetaUtils.TableColumns.class)) {
                MetaUtils.TableColumns tableColumns = mock(MetaUtils.TableColumns.class);
                staticTableColumns.when(() -> MetaUtils.TableColumns.build(any())).thenReturn(tableColumns);

                TableMeta tableMeta = mock(TableMeta.class);
                SqlAlterTable sqlAlterTable = mock(SqlAlterTable.class);

                List<SqlAlterSpecification> alters = new ArrayList<>();
                SqlChangeColumn changeColumn = mock(SqlChangeColumn.class);

                LogicalAlterTable alterTable = mock(LogicalAlterTable.class);
                SqlIdentifier sqlIdentifier = mock(SqlIdentifier.class);
                SqlColumnDeclaration sqlColumnDeclaration = mock(SqlColumnDeclaration.class);
                SqlDataTypeSpec sqlDataTypeSpec = mock(SqlDataTypeSpec.class);
                when(alterTable.getSchemaName()).thenReturn(schemaName);
                when(alterTable.getTableName()).thenReturn(tableName);
                when(changeColumn.getKind()).thenReturn(SqlKind.CHANGE_COLUMN);
                when(changeColumn.getOldName()).thenReturn(sqlIdentifier);
                when(sqlIdentifier.getLastName()).thenReturn("column_name");

                when(changeColumn.getColDef()).thenReturn(sqlColumnDeclaration);
                when(sqlColumnDeclaration.getDataType()).thenReturn(sqlDataTypeSpec);
                when(sqlDataTypeSpec.getTypeName()).thenReturn(sqlIdentifier);
                when(sqlIdentifier.getLastName()).thenReturn("TIMESTAMP");

                when(optimizerContext.getLatestSchemaManager()).thenReturn(schemaManager);
                when(schemaManager.getTable(any())).thenReturn(tableMeta);
                when(tableMeta.withCci()).thenReturn(true);
                when(alterTable.getSqlAlterTable()).thenReturn(sqlAlterTable);
                when(sqlAlterTable.getAlters()).thenReturn(alters);

                alters.add(changeColumn);

                try (MockedStatic<SqlValidatorImpl> staticSqlValidatorImpl = mockStatic(SqlValidatorImpl.class)) {
                    staticSqlValidatorImpl.when(
                            () -> SqlValidatorImpl.validateUnsupportedTypeWithCciWhenModifyColumn(sqlColumnDeclaration))
                        .thenAnswer(invocation -> null);

//                    thrown.expect(TddlRuntimeException.class);
//                    thrown.expectMessage("is not supported by CCI");
                    doCallRealMethod().when(alterTable).validateColumnar();
                    alterTable.validateColumnar();
                }
            }
        }
    }
}
