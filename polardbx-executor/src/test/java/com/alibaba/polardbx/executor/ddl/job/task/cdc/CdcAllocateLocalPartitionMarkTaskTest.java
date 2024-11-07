package com.alibaba.polardbx.executor.ddl.job.task.cdc;

import com.alibaba.polardbx.common.cdc.CdcDdlMarkVisibility;
import com.alibaba.polardbx.common.cdc.CdcManagerHelper;
import com.alibaba.polardbx.common.cdc.ICdcManager;
import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.calcite.sql.SqlKind;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class CdcAllocateLocalPartitionMarkTaskTest {

    @Test
    public void testMark4AllocateLocalPartitionTable() {

        ExecutionContext executionContext = Mockito.mock(ExecutionContext.class);
        ExecutionContext ecCopy = Mockito.mock(ExecutionContext.class);
        DdlContext ddlContext = new DdlContext();

        CdcAllocateLocalPartitionMarkTask cdcAllocateLocalPartitionMarkTask =
            Mockito.mock(CdcAllocateLocalPartitionMarkTask.class);

        Mockito.doNothing().when(
            cdcAllocateLocalPartitionMarkTask).updateSupportedCommands(false, false, null);
        Mockito.doCallRealMethod().when(cdcAllocateLocalPartitionMarkTask).duringTransaction(null, executionContext);
        Mockito.when(executionContext.copy()).thenReturn(ecCopy);
        Mockito.when(ecCopy.getDdlContext()).thenReturn(ddlContext);
        Mockito.when(executionContext.getDdlContext()).thenReturn(ddlContext);
        String alterTableStmt =
            "ALTER TABLE t_single_tb ALLOCATE LOCAL PARTITION";
        ddlContext.setDdlStmt(alterTableStmt);
        ddlContext.setDdlType(DdlType.ALTER_TABLE);

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
                    cdcAllocateLocalPartitionMarkTask.duringTransaction(null, executionContext);

                    verify(cdcManagerHelper, times(1)).notifyDdlNew(any(), any(),
                        eq(SqlKind.ALLOCATE_LOCAL_PARTITION.name()),
                        eq(alterTableStmt), eq(DdlType.ALTER_TABLE),
                        any(),
                        any(), eq(CdcDdlMarkVisibility.Protected), any());
                }
            }
        }
    }
}
