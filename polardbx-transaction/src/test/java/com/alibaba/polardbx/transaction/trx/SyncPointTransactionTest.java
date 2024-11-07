package com.alibaba.polardbx.transaction.trx;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.transaction.TransactionManager;
import com.alibaba.polardbx.transaction.tso.ClusterTimestampOracle;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SyncPointTransactionTest {
    @Test
    public void testSyncPoint() throws NoSuchFieldException, IllegalAccessException {
        ExecutionContext ctx = new ExecutionContext();
        TransactionManager transactionManager = mock(TransactionManager.class);
        ClusterTimestampOracle tso = mock(ClusterTimestampOracle.class);
        when(transactionManager.getTimestampOracle()).thenReturn(tso);

        SyncPointTransaction syncPointTransaction = new SyncPointTransaction(ctx, transactionManager);
        String primarySchema = "test_primary_schema";
        setParentVar(syncPointTransaction, AbstractTransaction.class, "primarySchema", primarySchema);
    }

    private static void setInstanceVar(Object obj, String fieldName, Object value)
        throws NoSuchFieldException, IllegalAccessException {
        java.lang.reflect.Field field = obj.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(obj, value);
    }

    private static void setParentVar(Object obj, Class parentClass, String fieldName, Object value)
        throws NoSuchFieldException, IllegalAccessException {
        java.lang.reflect.Field field = parentClass.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(obj, value);
    }
}
