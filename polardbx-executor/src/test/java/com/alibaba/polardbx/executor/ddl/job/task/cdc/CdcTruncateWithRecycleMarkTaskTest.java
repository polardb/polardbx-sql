package com.alibaba.polardbx.executor.ddl.job.task.cdc;

import com.alibaba.polardbx.common.cdc.CdcManagerHelper;
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.junit.Test;
import org.mockito.MockedStatic;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;

public class CdcTruncateWithRecycleMarkTaskTest {
    @Test
    public void testMark4RenameTable() {
        String schema = "test_schema";
        String sourceTable = "test_table";
        String targetTable = "test_target_table";

        CdcTruncateWithRecycleMarkTask cdcTruncateWithRecycleMarkTask =
            new CdcTruncateWithRecycleMarkTask(schema, sourceTable, targetTable);
        CdcManagerHelper cdcManagerHelper = mock(CdcManagerHelper.class);
        try (MockedStatic<TableMetaChanger> tableMetaChangerMockedStatic = mockStatic(TableMetaChanger.class);
            MockedStatic<CdcManagerHelper> cdcManagerHelperMockedStatic = mockStatic(CdcManagerHelper.class)) {
            tableMetaChangerMockedStatic
                .when(() -> TableMetaChanger.buildNewTbNamePattern(new ExecutionContext(), schema, sourceTable,
                    targetTable, false))
                .thenReturn(targetTable);
            cdcManagerHelperMockedStatic.when(() -> CdcManagerHelper.getInstance()).thenReturn(cdcManagerHelper);
            ExecutionContext executionContext = new ExecutionContext(schema);
            executionContext.setDdlContext(new DdlContext());

            cdcTruncateWithRecycleMarkTask.mark4RenameTable(executionContext);

            tableMetaChangerMockedStatic.verify(
                () -> TableMetaChanger.buildNewTbNamePattern(eq(executionContext), eq(schema), eq(sourceTable),
                    eq(targetTable), eq(false)), times(1));
        }
    }
}
