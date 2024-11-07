package com.alibaba.polardbx.transaction.tso;

import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.executor.mpp.Session;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.transaction.ColumnarTransaction;
import com.alibaba.polardbx.transaction.TransactionManager;
import com.alibaba.polardbx.transaction.trx.AutoCommitSingleShardTsoTransaction;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.SQLException;
import java.util.HashMap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class SessionTest {

    @Test
    public void testColumnarGetTso() throws SQLException {
        ExecutionContext context = new ExecutionContext();
        context.getExtraCmds().put(ConnectionProperties.SNAPSHOT_TS, 1);
        ColumnarTransaction transaction = mock(ColumnarTransaction.class);
        Mockito.when(transaction.getTransactionClass()).thenReturn(
            ITransactionPolicy.TransactionClass.COLUMNAR_READ_ONLY_TRANSACTION);
        context.setTransaction(transaction);
        Session session = new Session("session", context);
        session.generateTsoInfo();
        Assert.assertTrue(session.getTsoTime() == 1L);
    }

    @Test
    public void testNonColumnarGetTso() throws SQLException {

        ExecutionContext executionContext = new ExecutionContext("polardbx");
        executionContext.getExtraCmds().put(ConnectionProperties.SNAPSHOT_TS, 1);
        executionContext.getExtraCmds().put(ConnectionProperties.ENABLE_CONSISTENT_REPLICA_READ, true);

        ExecutorContext ec = mock(ExecutorContext.class);

        TransactionManager transactionManager = spy(new TransactionManager());
        StorageInfoManager mockStorageManager = mock(StorageInfoManager.class);
        transactionManager.prepare("polardbx", new HashMap<>(), mockStorageManager);
        when(mockStorageManager.supportTso()).thenReturn(true);
        when(ec.getStorageInfoManager()).thenReturn(mockStorageManager);

        try (MockedStatic<ExecutorContext> mockedEc = mockStatic(ExecutorContext.class)) {
            mockedEc.when(() -> ExecutorContext.getContext(any())).thenReturn(ec);
            AutoCommitSingleShardTsoTransaction transaction = spy(new AutoCommitSingleShardTsoTransaction(
                executionContext, transactionManager, false, true));
            executionContext.setTransaction(transaction);
            Session session = new Session("session", executionContext);
            session.generateTsoInfo();
            Assert.assertTrue(session.getTsoTime() == 1L);
        }
    }

}
