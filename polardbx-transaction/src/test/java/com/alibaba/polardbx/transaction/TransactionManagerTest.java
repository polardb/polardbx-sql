package com.alibaba.polardbx.transaction;

import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.transaction.trx.AsyncCommitTransaction;
import com.alibaba.polardbx.transaction.trx.AutoCommitTransaction;
import com.alibaba.polardbx.transaction.trx.AutoCommitTsoTransaction;
import com.alibaba.polardbx.transaction.trx.TsoTransaction;
import com.alibaba.polardbx.transaction.trx.XATransaction;
import com.alibaba.polardbx.transaction.trx.XATsoTransaction;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransactionManagerTest {

    @Test
    public void testCreateTransactionWithXA() {
        Map<String, String> properties = new HashMap<>();
        ExecutionContext mockExecutionContext = mock(ExecutionContext.class);
        StorageInfoManager mockStorageManager = mock(StorageInfoManager.class);
        TransactionManager transactionManager = new TransactionManager();
        transactionManager.prepare("polardbx", new HashMap<>(), mockStorageManager);
        when(mockExecutionContext.getParamManager()).thenReturn(new ParamManager(properties));
        when(mockStorageManager.supportLizard1PCTransaction()).thenReturn(false);
        when(mockStorageManager.supportCtsTransaction()).thenReturn(false);
        ITransactionPolicy.TransactionClass trxConfig = ITransactionPolicy.TransactionClass.XA;

        // Enable XA_TSO iff DN is supported and option is on.
        when(mockStorageManager.isSupportMarkDistributed()).thenReturn(true);
        when(mockExecutionContext.isEnableXaTso()).thenReturn(true);
        ITransaction result = transactionManager.createTransaction(trxConfig, mockExecutionContext);
        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof XATsoTransaction);

        when(mockStorageManager.isSupportMarkDistributed()).thenReturn(true);
        when(mockExecutionContext.isEnableXaTso()).thenReturn(false);
        result = transactionManager.createTransaction(trxConfig, mockExecutionContext);
        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof XATransaction);

        when(mockStorageManager.isSupportMarkDistributed()).thenReturn(false);
        when(mockExecutionContext.isEnableXaTso()).thenReturn(true);
        result = transactionManager.createTransaction(trxConfig, mockExecutionContext);
        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof XATransaction);
    }

    @Test
    public void testCreateTransactionWithTSO() {
        Map<String, String> properties = new HashMap<>();
        ExecutionContext mockExecutionContext = mock(ExecutionContext.class);
        StorageInfoManager mockStorageManager = mock(StorageInfoManager.class);
        TransactionManager transactionManager = new TransactionManager();
        transactionManager.prepare("polardbx", new HashMap<>(), mockStorageManager);
        when(mockExecutionContext.getParamManager()).thenReturn(new ParamManager(properties));
        when(mockStorageManager.supportLizard1PCTransaction()).thenReturn(true);
        when(mockStorageManager.supportCtsTransaction()).thenReturn(true);
        ITransactionPolicy.TransactionClass trxConfig = ITransactionPolicy.TransactionClass.TSO;

        when(mockExecutionContext.enableAsyncCommit()).thenReturn(false);
        when(mockStorageManager.supportAsyncCommit()).thenReturn(false);
        ITransaction result = transactionManager.createTransaction(trxConfig, mockExecutionContext);
        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof TsoTransaction);

        when(mockExecutionContext.enableAsyncCommit()).thenReturn(true);
        when(mockStorageManager.supportAsyncCommit()).thenReturn(false);
        result = transactionManager.createTransaction(trxConfig, mockExecutionContext);
        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof TsoTransaction);

        when(mockExecutionContext.enableAsyncCommit()).thenReturn(true);
        when(mockStorageManager.supportAsyncCommit()).thenReturn(true);
        result = transactionManager.createTransaction(trxConfig, mockExecutionContext);
        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof AsyncCommitTransaction);
    }

    @Test
    public void testCreateTransactionWithAutocommit() {
        Map<String, String> properties = new HashMap<>();
        ExecutionContext mockExecutionContext = mock(ExecutionContext.class);
        StorageInfoManager mockStorageManager = mock(StorageInfoManager.class);
        TransactionManager transactionManager = new TransactionManager();
        transactionManager.prepare("polardbx", new HashMap<>(), mockStorageManager);
        when(mockExecutionContext.getParamManager()).thenReturn(new ParamManager(properties));
        when(mockStorageManager.supportLizard1PCTransaction()).thenReturn(true);
        when(mockStorageManager.supportCtsTransaction()).thenReturn(true);
        ITransactionPolicy.TransactionClass trxConfig = ITransactionPolicy.TransactionClass.AUTO_COMMIT;

        when(mockStorageManager.isSupportMarkDistributed()).thenReturn(true);
        when(mockExecutionContext.isEnableAutoCommitTso()).thenReturn(true);
        ITransaction result = transactionManager.createTransaction(trxConfig, mockExecutionContext);
        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof AutoCommitTsoTransaction);

        when(mockStorageManager.isSupportMarkDistributed()).thenReturn(true);
        when(mockExecutionContext.isEnableAutoCommitTso()).thenReturn(false);
        result = transactionManager.createTransaction(trxConfig, mockExecutionContext);
        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof AutoCommitTransaction);

        when(mockStorageManager.isSupportMarkDistributed()).thenReturn(false);
        when(mockExecutionContext.isEnableAutoCommitTso()).thenReturn(true);
        result = transactionManager.createTransaction(trxConfig, mockExecutionContext);
        Assert.assertNotNull(result);
        Assert.assertTrue(result instanceof AutoCommitTransaction);
    }

}
