package com.alibaba.polardbx.transaction.trx;

import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.transaction.TransactionManager;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;

import static org.mockito.Mockito.mock;

public class ArchiveTransactionTest {
    @Test
    public void testShareReadview() {
        TransactionManager transactionManager = new MockTransactionManager();
        ExecutionContext ec = new ExecutionContext();
        ec.setTxIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        ec.setShareReadView(true);
        ITransaction trx = transactionManager.createTransaction(ITransactionPolicy.TransactionClass.ARCHIVE, ec);
        Assert.assertTrue(trx instanceof ArchiveTransaction);
        Assert.assertEquals(ITransactionPolicy.TransactionClass.ARCHIVE, trx.getTransactionClass());
        Assert.assertTrue(((ArchiveTransaction) trx).getTrxLoggerPrefix()
            .contains(ITransactionPolicy.TransactionClass.ARCHIVE.name()));
        IConnection connection = mock(IConnection.class);
        String xid = ((ArchiveTransaction) trx).getXid("group", connection);
        Assert.assertTrue(xid.endsWith("'group@0000', 3"));
        xid = ((ArchiveTransaction) trx).getXid("group", connection);
        Assert.assertTrue(xid.endsWith("'group@0001', 3"));
    }

    @Test
    public void testNoShareReadview() {
        TransactionManager transactionManager = new MockTransactionManager();
        ExecutionContext ec = new ExecutionContext();
        ec.setTxIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        ec.setShareReadView(false);
        ITransaction trx = transactionManager.createTransaction(ITransactionPolicy.TransactionClass.ARCHIVE, ec);
        Assert.assertTrue(trx instanceof ArchiveTransaction);
        Assert.assertEquals(ITransactionPolicy.TransactionClass.ARCHIVE, trx.getTransactionClass());
        Assert.assertTrue(((ArchiveTransaction) trx).getTrxLoggerPrefix()
            .contains(ITransactionPolicy.TransactionClass.ARCHIVE.name()));
        IConnection connection = mock(IConnection.class);
        String xid = ((ArchiveTransaction) trx).getXid("group", connection);
        Assert.assertTrue(xid.endsWith("'group', 3"));
        xid = ((ArchiveTransaction) trx).getXid("group", connection);
        Assert.assertTrue(xid.endsWith("'group', 3"));
    }

    private static class MockTransactionManager extends TransactionManager {
        public void enableKillTimeoutTransaction() {
            // do nothing
        }

        public void enableLogCleanTask() {
            // do nothing
        }
    }
}
