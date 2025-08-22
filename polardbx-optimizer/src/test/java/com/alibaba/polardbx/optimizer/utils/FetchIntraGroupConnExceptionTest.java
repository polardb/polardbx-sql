package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.DirectMultiDBTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.SingleTableInsert;
import com.alibaba.polardbx.optimizer.core.rel.SingleTableOperation;
import com.alibaba.polardbx.optimizer.partition.PartSpecSearcher;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSelect;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * @author chenghui.lch
 */
public class FetchIntraGroupConnExceptionTest {

    @Test
    public void testExceptionMsg() {

        try {
            PhyTableOperationUtil.throwFetchGroupConnFailedException(100L, null, "phydb1", "phytb1");
        } catch (Throwable ex) {
            String msg = ex.getMessage();
            System.out.println(msg);
            Assert.assertTrue(msg.contains("phydb1"));
            Assert.assertTrue(msg.contains("phytb1"));
        }

        try {
            PhyTableOperationUtil.throwFetchGroupConnFailedException(null, null, "phydb1", "phytb1");
        } catch (Throwable ex) {
            String msg = ex.getMessage();
            System.out.println(msg);
            Assert.assertTrue(msg.contains("phydb1"));
            Assert.assertTrue(msg.contains("phytb1"));
        }

    }

    @Test
    public void fetchBasePhyOpIntraGroupConnKeyTest() {
        try (MockedStatic<DbInfoManager> dbInfoManagerMockedStatic = mockStatic(DbInfoManager.class)) {
            ExecutionContext ec = mock(ExecutionContext.class);
            ITransaction trx = mock(ITransaction.class);
            ec.setTransaction(trx);
            String schemaName = "test";
            List<String> logTblNames = Collections.singletonList("test");

            DbInfoManager dm = mock(DbInfoManager.class);
            when(dm.isNewPartitionDb(schemaName)).thenReturn(true);
            dbInfoManagerMockedStatic.when(DbInfoManager::getInstance).thenReturn(dm);

            SchemaManager sm = mock(SchemaManager.class);
            ec.setSchemaManager(schemaName, sm);
            TableMeta tbMeta = mock(TableMeta.class);
            when(sm.getTable(anyString())).thenReturn(tbMeta);

            TddlRuleManager ruleManager = mock(TddlRuleManager.class);
            when(sm.getTddlRuleManager()).thenReturn(ruleManager);

            PartitionInfoManager partitionInfoManager = mock(PartitionInfoManager.class);
            when(ruleManager.getPartitionInfoManager()).thenReturn(partitionInfoManager);
            when(partitionInfoManager.isPartitionedTable(any())).thenReturn(true);
            PartitionInfo partInfo = mock(PartitionInfo.class);
            when(partitionInfoManager.getPartitionInfo(any())).thenReturn(partInfo);
            when(tbMeta.getNewPartitionInfo()).thenReturn(partInfo);
            PartSpecSearcher specSearcher = mock(PartSpecSearcher.class);
            when(partInfo.getPartSpecSearcher()).thenReturn(specSearcher);
            when(specSearcher.getPartIntraGroupConnKey(any(), any())).thenReturn(PartSpecSearcher.NO_FOUND_PART_SPEC);
            ParamManager paramManager = mock(ParamManager.class);
            ec.setParamManager(paramManager);
            when(paramManager.getBoolean(ConnectionParams.ENABLE_LOG_GROUP_CONN_KEY)).thenReturn(false);
            when(ec.getParamManager()).thenReturn(paramManager);
            when(paramManager.getBoolean(ConnectionParams.ENABLE_GROUP_PARALLELISM)).thenReturn(true);
            when(ec.getGroupParallelism()).thenReturn(8L);
            when(ec.getTransaction()).thenReturn(trx);
            when(trx.getTransactionClass()).thenReturn(ITransactionPolicy.TransactionClass.TSO);
            when(ec.getSchemaManager("test")).thenReturn(sm);
            when(sm.getTable(any())).thenReturn(tbMeta);

            Long key = 0L;
            List<List<String>> phyTables = new ArrayList<>();
            phyTables.add(logTblNames);

            // PhyTableOperation
            try {
                PhyTableOperation op = mock(PhyTableOperation.class);
                when(op.getKind()).thenReturn(SqlKind.INSERT);
                when(op.getSchemaName()).thenReturn(schemaName);
                when(op.getTableNames()).thenReturn(phyTables);
                when(op.getLogicalTableNames()).thenReturn(logTblNames);
                when(op.getDbIndex()).thenReturn("nodb");
                when(op.isReplicateRelNode()).thenReturn(true);
                when(op.getKind()).thenReturn(SqlKind.SELECT);
                when(op.getLockMode()).thenReturn(SqlSelect.LockMode.UNDEF);
                key = PhyTableOperationUtil.fetchBaseOpIntraGroupConnKey(op, op.getDbIndex(), op.getTableNames(), ec);
                Assert.fail("show throw exception");
            } catch (Throwable ex) {
                String msg = ex.getMessage();
                Assert.assertTrue(msg.contains("invalid group or physical table names"));
            }

            // SingleTableOperation
            try {
                SingleTableOperation op = mock(SingleTableOperation.class);
                when(op.getKind()).thenReturn(SqlKind.SELECT);
                when(op.getSchemaName()).thenReturn(schemaName);
                when(op.getTableNames()).thenReturn(phyTables.get(0));
                when(op.getLogicalTableNames()).thenReturn(logTblNames);
                when(op.getDbIndex()).thenReturn("nodb");
                when(op.isReplicateRelNode()).thenReturn(true);
                key = PhyTableOperationUtil.fetchBaseOpIntraGroupConnKey(op, "nodb", phyTables, ec);
                Assert.fail("show throw exception");
            } catch (Throwable ex) {
                String msg = ex.getMessage();
                Assert.assertTrue(msg.contains("invalid group or physical table names"));
            }

            // SingleTableOperation
            try {
                SingleTableInsert op = mock(SingleTableInsert.class);
                when(op.getKind()).thenReturn(SqlKind.INSERT);
                when(op.getSchemaName()).thenReturn(schemaName);
                when(op.getTableNames()).thenReturn(phyTables.get(0));
                when(op.getLogicalTableNames()).thenReturn(logTblNames);
                when(op.getDbIndex()).thenReturn("nodb");
                when(op.isReplicateRelNode()).thenReturn(true);
                key = PhyTableOperationUtil.fetchBaseOpIntraGroupConnKey(op, "nodb", phyTables, ec);
                Assert.fail("show throw exception");
            } catch (Throwable ex) {
                String msg = ex.getMessage();
                Assert.assertTrue(msg.contains("invalid group or physical table names"));
            }

            // DirectMultiDBTableOperation
            try {
                DirectMultiDBTableOperation op = mock(DirectMultiDBTableOperation.class);
                when(op.getKind()).thenReturn(SqlKind.SELECT);
                when(op.getBaseSchemaName(ec)).thenReturn(schemaName);
                when(op.getSchemaName()).thenReturn(schemaName);
                when(op.getLogicalTables(schemaName)).thenReturn(logTblNames);
                when(op.getLogicalTableNames()).thenReturn(logTblNames);
                when(op.getPhysicalTables(schemaName)).thenReturn(logTblNames);
                when(op.isReplicateRelNode()).thenReturn(true);
                key = PhyTableOperationUtil.fetchBaseOpIntraGroupConnKey(op, "test", phyTables, ec);
                Assert.fail("show throw exception");
            } catch (Throwable ex) {
                String msg = ex.getMessage();
                Assert.assertTrue(msg.contains("invalid group or physical table names"));
            }

            // PhyQueryOperation
            try {
                PhyQueryOperation op = mock(PhyQueryOperation.class);
                when(op.getKind()).thenReturn(SqlKind.SELECT);
                when(op.getSchemaName()).thenReturn(schemaName);
                when(op.getPhysicalTables()).thenReturn(phyTables);
                when(op.getDbIndex()).thenReturn("nodb");
                when(op.isReplicateRelNode()).thenReturn(true);
                when(op.getKind()).thenReturn(SqlKind.SELECT);
                when(op.getLockMode()).thenReturn(SqlSelect.LockMode.UNDEF);
                when(op.isUsingConnKey()).thenReturn(true);
                PhyTableOperationUtil.fetchBaseOpIntraGroupConnKey(op, op.getDbIndex(), phyTables, ec);
                Assert.fail("show throw exception");
            } catch (Throwable ex) {
                String msg = ex.getMessage();
                Assert.assertTrue(msg.contains("invalid physical table names"));
            }
        }
    }

}
