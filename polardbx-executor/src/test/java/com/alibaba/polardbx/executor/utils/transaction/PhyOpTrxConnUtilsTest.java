package com.alibaba.polardbx.executor.utils.transaction;

import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PhyOpTrxConnUtilsTest {
    @Test
    public void test0() throws SQLException {
        ITransaction trx = mock(ITransaction.class);
        TGroupDataSource grpDs = mock(TGroupDataSource.class);
        String schemaName = "test_schema";
        String groupName = "test_group";
        ITransaction.RW rw = ITransaction.RW.READ;
        Long grpConnId = 123L;
        ExecutionContext ec = new ExecutionContext();
        IConnection writeParallelConnection = mock(IConnection.class);
        IConnection defaultConnection = mock(IConnection.class);

        when(trx.getConnection(schemaName, groupName, grpConnId, grpDs, rw, ec)).thenReturn(writeParallelConnection);
        when(trx.getConnection(schemaName, groupName, grpDs, rw, ec)).thenReturn(defaultConnection);

        // Not support:
        when(trx.getTransactionClass()).thenReturn(ITransactionPolicy.TransactionClass.AUTO_COMMIT);
        Assert.assertEquals(defaultConnection,
            PhyOpTrxConnUtils.getConnection(trx, schemaName, groupName, grpDs, rw, ec, grpConnId));

        when(trx.getTransactionClass()).thenReturn(ITransactionPolicy.TransactionClass.AUTO_COMMIT_TSO);
        Assert.assertEquals(defaultConnection,
            PhyOpTrxConnUtils.getConnection(trx, schemaName, groupName, grpDs, rw, ec, grpConnId));

        when(trx.getTransactionClass()).thenReturn(ITransactionPolicy.TransactionClass.AUTO_COMMIT_SINGLE_SHARD);
        Assert.assertEquals(defaultConnection,
            PhyOpTrxConnUtils.getConnection(trx, schemaName, groupName, grpDs, rw, ec, grpConnId));

        when(trx.getTransactionClass()).thenReturn(ITransactionPolicy.TransactionClass.BEST_EFFORT);
        Assert.assertEquals(defaultConnection,
            PhyOpTrxConnUtils.getConnection(trx, schemaName, groupName, grpDs, rw, ec, grpConnId));

        when(trx.getTransactionClass()).thenReturn(ITransactionPolicy.TransactionClass.TSO_READONLY);
        Assert.assertEquals(defaultConnection,
            PhyOpTrxConnUtils.getConnection(trx, schemaName, groupName, grpDs, rw, ec, grpConnId));

        when(trx.getTransactionClass()).thenReturn(ITransactionPolicy.TransactionClass.ALLOW_READ_CROSS_DB);
        Assert.assertEquals(defaultConnection,
            PhyOpTrxConnUtils.getConnection(trx, schemaName, groupName, grpDs, rw, ec, grpConnId));

        when(trx.getTransactionClass()).thenReturn(ITransactionPolicy.TransactionClass.COBAR_STYLE);
        Assert.assertEquals(defaultConnection,
            PhyOpTrxConnUtils.getConnection(trx, schemaName, groupName, grpDs, rw, ec, grpConnId));

        when(trx.getTransactionClass()).thenReturn(ITransactionPolicy.TransactionClass.MPP_READ_ONLY_TRANSACTION);
        Assert.assertEquals(defaultConnection,
            PhyOpTrxConnUtils.getConnection(trx, schemaName, groupName, grpDs, rw, ec, grpConnId));

        // Support:
        when(trx.getTransactionClass()).thenReturn(ITransactionPolicy.TransactionClass.XA);
        Assert.assertEquals(writeParallelConnection,
            PhyOpTrxConnUtils.getConnection(trx, schemaName, groupName, grpDs, rw, ec, grpConnId));

        when(trx.getTransactionClass()).thenReturn(ITransactionPolicy.TransactionClass.XA_TSO);
        Assert.assertEquals(writeParallelConnection,
            PhyOpTrxConnUtils.getConnection(trx, schemaName, groupName, grpDs, rw, ec, grpConnId));

        when(trx.getTransactionClass()).thenReturn(ITransactionPolicy.TransactionClass.TSO);
        Assert.assertEquals(writeParallelConnection,
            PhyOpTrxConnUtils.getConnection(trx, schemaName, groupName, grpDs, rw, ec, grpConnId));
    }
}
