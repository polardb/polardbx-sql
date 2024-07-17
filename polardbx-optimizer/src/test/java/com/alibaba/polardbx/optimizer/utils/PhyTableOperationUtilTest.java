package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.DirectMultiDBTableOperation;
import com.alibaba.polardbx.optimizer.partition.PartSpecSearcher;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import org.apache.calcite.sql.SqlKind;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class PhyTableOperationUtilTest {
    @Test
    public void controlIntraGroupParallelismTest() {
        ExecutionContext ec = new ExecutionContext();
        ITransaction trx = mock(ITransaction.class);
        ec.setTransaction(trx);
        String schemaName = "test";
        DbInfoManager dm = mock(DbInfoManager.class);
        when(dm.isNewPartitionDb(schemaName)).thenReturn(true);

        try (MockedStatic<DbInfoManager> dbInfoManagerMockedStatic = mockStatic(DbInfoManager.class)) {
            dbInfoManagerMockedStatic.when(DbInfoManager::getInstance).thenReturn(dm);

            assertEnable(ec, trx, schemaName, ITransactionPolicy.TransactionClass.TSO);

            assertDisable(ec, trx, schemaName, ITransactionPolicy.TransactionClass.TSO);

            assertEnable(ec, trx, schemaName, ITransactionPolicy.TransactionClass.XA);

            assertDisable(ec, trx, schemaName, ITransactionPolicy.TransactionClass.XA);

            assertEnable(ec, trx, schemaName, ITransactionPolicy.TransactionClass.XA_TSO);

            assertDisable(ec, trx, schemaName, ITransactionPolicy.TransactionClass.XA_TSO);

            assertEnable(ec, trx, schemaName, ITransactionPolicy.TransactionClass.AUTO_COMMIT);

            assertEnable(ec, trx, schemaName, ITransactionPolicy.TransactionClass.AUTO_COMMIT_TSO);

            assertEnable(ec, trx, schemaName, ITransactionPolicy.TransactionClass.TSO_READONLY);
        }
    }

    private static void assertEnable(ExecutionContext ec, ITransaction trx, String schemaName,
                                     ITransactionPolicy.TransactionClass txClass) {
        ec.setExtraCmds(new HashMap<>());
        ec.setTxIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        ec.setShareReadView(true);
        when(trx.getTransactionClass()).thenReturn(txClass);
        PhyTableOperationUtil.controlIntraGroupParallelism(schemaName, ec, true);
        Assert.assertTrue((Boolean) ec.getExtraCmds().get(ConnectionProperties.ENABLE_GROUP_PARALLELISM));
    }

    private static void assertDisable(ExecutionContext ec, ITransaction trx, String schemaName,
                                      ITransactionPolicy.TransactionClass txClass) {
        ec.setExtraCmds(new HashMap<>());
        ec.setTxIsolation(Connection.TRANSACTION_READ_COMMITTED);
        when(trx.getTransactionClass()).thenReturn(txClass);
        PhyTableOperationUtil.controlIntraGroupParallelism(schemaName, ec, true);
        Assert.assertNull(ec.getExtraCmds().get(ConnectionProperties.ENABLE_GROUP_PARALLELISM));
    }

    @Test
    public void fetchMultiDBIntraGroupConnKeyTest() {
        try (MockedStatic<DbInfoManager> dbInfoManagerMockedStatic = mockStatic(DbInfoManager.class)) {
            ExecutionContext ec = new ExecutionContext();
            ITransaction trx = mock(ITransaction.class);
            ec.setTransaction(trx);
            String schemaName = "test";
            DbInfoManager dm = mock(DbInfoManager.class);
            when(dm.isNewPartitionDb(schemaName)).thenReturn(true);
            dbInfoManagerMockedStatic.when(DbInfoManager::getInstance).thenReturn(dm);
            DirectMultiDBTableOperation op = mock(DirectMultiDBTableOperation.class);
            when(op.getKind()).thenReturn(SqlKind.INSERT);
            when(op.getBaseSchemaName(ec)).thenReturn(schemaName);
            when(op.getSchemaName()).thenReturn(schemaName);
            List<String> logTblNames = Collections.singletonList("test");
            when(op.getLogicalTables(schemaName)).thenReturn(logTblNames);
            when(op.getLogicalTableNames()).thenReturn(logTblNames);
            when(op.getPhysicalTables(schemaName)).thenReturn(logTblNames);
            SchemaManager sm = mock(SchemaManager.class);
            ec.setSchemaManager(schemaName, sm);
            TableMeta tbMeta = mock(TableMeta.class);
            when(sm.getTable(anyString())).thenReturn(tbMeta);
            when(op.isReplicateRelNode()).thenReturn(true);
            PartitionInfo partInfo = mock(PartitionInfo.class);
            when(tbMeta.getNewPartitionInfo()).thenReturn(partInfo);
            PartSpecSearcher specSearcher = mock(PartSpecSearcher.class);
            when(partInfo.getPartSpecSearcher()).thenReturn(specSearcher);
            when(specSearcher.getPartIntraGroupConnKey(any(), any())).thenReturn(10L);
            ParamManager paramManager = mock(ParamManager.class);
            ec.setParamManager(paramManager);
            when(paramManager.getBoolean(ConnectionParams.ENABLE_LOG_GROUP_CONN_KEY)).thenReturn(false);

            when(trx.getTransactionClass()).thenReturn(ITransactionPolicy.TransactionClass.TSO);
            Long key = PhyTableOperationUtil.fetchMultiDBIntraGroupConnKey(op, schemaName, ec);
            Assert.assertEquals(Long.valueOf(10), key);

            when(trx.getTransactionClass()).thenReturn(ITransactionPolicy.TransactionClass.XA);
            key = PhyTableOperationUtil.fetchMultiDBIntraGroupConnKey(op, schemaName, ec);
            Assert.assertEquals(Long.valueOf(10), key);

            when(trx.getTransactionClass()).thenReturn(ITransactionPolicy.TransactionClass.XA_TSO);
            key = PhyTableOperationUtil.fetchMultiDBIntraGroupConnKey(op, schemaName, ec);
            Assert.assertEquals(Long.valueOf(10), key);

            when(trx.getTransactionClass()).thenReturn(ITransactionPolicy.TransactionClass.AUTO_COMMIT);
            key = PhyTableOperationUtil.fetchMultiDBIntraGroupConnKey(op, schemaName, ec);
            Assert.assertNull(key);

            List<List<String>> phyTables = new ArrayList<>();
            phyTables.add(logTblNames);

            when(trx.getTransactionClass()).thenReturn(ITransactionPolicy.TransactionClass.TSO);
            key = PhyTableOperationUtil.fetchNonPhyOpIntraGroupConnKey(op, "test", phyTables, ec);
            Assert.assertEquals(Long.valueOf(10), key);

            when(trx.getTransactionClass()).thenReturn(ITransactionPolicy.TransactionClass.XA);
            key = PhyTableOperationUtil.fetchNonPhyOpIntraGroupConnKey(op, "test", phyTables, ec);
            Assert.assertEquals(Long.valueOf(10), key);

            when(trx.getTransactionClass()).thenReturn(ITransactionPolicy.TransactionClass.XA_TSO);
            key = PhyTableOperationUtil.fetchNonPhyOpIntraGroupConnKey(op, "test", phyTables, ec);
            Assert.assertEquals(Long.valueOf(10), key);

            when(trx.getTransactionClass()).thenReturn(ITransactionPolicy.TransactionClass.AUTO_COMMIT);
            key = PhyTableOperationUtil.fetchNonPhyOpIntraGroupConnKey(op, "test", phyTables, ec);
            Assert.assertNull(key);
        }
    }
}
