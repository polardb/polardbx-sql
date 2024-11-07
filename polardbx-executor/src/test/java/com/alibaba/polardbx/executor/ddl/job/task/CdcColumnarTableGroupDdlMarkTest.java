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

package com.alibaba.polardbx.executor.ddl.job.task;

import com.alibaba.polardbx.common.cdc.CdcDdlMarkVisibility;
import com.alibaba.polardbx.common.cdc.CdcManagerHelper;
import com.alibaba.polardbx.common.cdc.ICdcManager;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcColumnarTableGroupDdlMark;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcMarkUtil;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class CdcColumnarTableGroupDdlMarkTest {

    @InjectMocks
    private CdcColumnarTableGroupDdlMark cdcColumnarTableGroupDdlMark;

    @Mock
    private ExecutionContext executionContext;

    @Mock
    private ExecutionContext ecCopy;

    private final DdlContext ddlContext = new DdlContext();

    @Before
    public void setUp() {
        cdcColumnarTableGroupDdlMark =
            new CdcColumnarTableGroupDdlMark("tg_test", "test_schema", Collections.singletonList("test_table"), 1L);
        Mockito.when(executionContext.copy()).thenReturn(ecCopy);
        Mockito.when(ecCopy.getDdlContext()).thenReturn(ddlContext);
    }

    @Test
    public void testMark4ColumnarTableGroupChangeWithAlterTable() {
        String alterTableStmt = "alter table `test_table` drop partition `p1`";
        ddlContext.setDdlStmt(alterTableStmt);

        Map<String, Object> extendParams = new HashMap<>();
        extendParams.put(ICdcManager.CDC_IS_CCI, true);

        SQLAlterTableStatement alterTableStatement = new SQLAlterTableStatement();
        List<SQLAlterTableStatement> parseResult = Collections.singletonList(alterTableStatement);
        try (MockedStatic<SQLUtils> sqlUtilsMock = Mockito.mockStatic(SQLUtils.class)) {
            sqlUtilsMock.when(
                    () -> SQLUtils.parseStatements(eq(alterTableStmt), any(), any()))
                .thenReturn(parseResult);

            try (MockedStatic<CdcManagerHelper> cdcManagerMock = Mockito.mockStatic(CdcManagerHelper.class)) {
                CdcManagerHelper cdcManagerHelper = Mockito.mock(CdcManagerHelper.class);
                cdcManagerMock.when(CdcManagerHelper::getInstance).thenReturn(cdcManagerHelper);

                try (MockedStatic<CdcMarkUtil> cdcMarkUtilMock = Mockito.mockStatic(CdcMarkUtil.class)) {
                    cdcMarkUtilMock.when(() -> CdcMarkUtil.buildExtendParameter(executionContext))
                        .thenReturn(extendParams);

                    cdcColumnarTableGroupDdlMark.mark4ColumnarTableGroupChange(executionContext);

                    verify(cdcManagerHelper, times(1)).notifyDdlNew(eq("test_schema"), eq("test_table"),
                        eq("ALTER_TABLE"),
                        eq(alterTableStmt), eq(com.alibaba.polardbx.common.ddl.newengine.DdlType.ALTER_TABLEGROUP),
                        any(),
                        any(), eq(CdcDdlMarkVisibility.Protected), any());
                }
            }
        }
    }
}
