package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.logical.ITConnection;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.BaseTableOperation;
import com.alibaba.polardbx.repo.mysql.spi.MyJdbcHandler;
import com.alibaba.polardbx.repo.mysql.spi.MyRepository;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MySingleTableModifyReturningHandlerTest {
    @Test
    public void testWithReturningSequentialPolicy() throws NoSuchFieldException, IllegalAccessException {
        MyRepository repo = new MockMyRepository();
        MySingleTableModifyReturningHandler handler = new MySingleTableModifyReturningHandler(repo);
        ExecutionContext executionContext = new ExecutionContext();
        executionContext.setReturning("returning");
        executionContext.getParamManager().getProps().put(ConnectionProperties.SEQUENTIAL_CONCURRENT_POLICY, "true");

        BaseTableOperation logicalPlan = mock(BaseTableOperation.class);

        long oldLastInsertId = 1L;
        ITConnection connection = mock(ITConnection.class);
        executionContext.setConnection(connection);
        when(connection.getLastInsertId()).thenReturn(oldLastInsertId);
        Cursor cursor = handler.handleInner(logicalPlan, executionContext);
        Field field = cursor.getClass().getSuperclass().getSuperclass().getSuperclass().getDeclaredField("inited");
        field.setAccessible(true);
        // Should delay init.
        Assert.assertFalse((Boolean) field.get(cursor));
    }

    private static class MockMyRepository extends MyRepository {
        protected MyJdbcHandler createQueryHandler(ExecutionContext executionContext) {
            return mock(MyJdbcHandler.class);
        }
    }
}
