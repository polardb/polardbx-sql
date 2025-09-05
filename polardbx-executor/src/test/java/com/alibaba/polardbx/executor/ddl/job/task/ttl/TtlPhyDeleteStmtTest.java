package com.alibaba.polardbx.executor.ddl.job.task.ttl;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.gms.ttl.TtlInfoRecord;
import com.alibaba.polardbx.optimizer.ttl.TtlDefinitionInfo;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * @author chenghui.lch
 */
public class TtlPhyDeleteStmtTest {

    @Test
    public void testPhyDeletingStmt() throws Throwable {
        try {
            TtlInfoRecord mockTtlRec = Mockito.mock(TtlInfoRecord.class);
            TtlDefinitionInfo mockTtlInfo = Mockito.mock(TtlDefinitionInfo.class);
            Mockito.when(mockTtlRec.getTableSchema()).thenReturn("db1");
            Mockito.when(mockTtlRec.getTableName()).thenReturn("tb1");
            Mockito.when(mockTtlRec.getTtlCol()).thenReturn("b");
            Mockito.when(mockTtlInfo.getTtlInfoRecord()).thenReturn(mockTtlRec);

            boolean needAddIntervalLowerBound = false;
            String queryHint = "";
            String forceIndexExpr = "force index(`idx_b`)";
            String phyDeleteStmt =
                TtlTaskSqlBuilder.buildDeleteTemplate(mockTtlInfo, needAddIntervalLowerBound, queryHint,
                    forceIndexExpr);
            System.out.println(phyDeleteStmt);
            Assert.assertTrue(!phyDeleteStmt.toLowerCase().contains("force index"));
        } catch (Throwable ex) {
            Assert.fail(ex.getMessage());
        }

    }
}
