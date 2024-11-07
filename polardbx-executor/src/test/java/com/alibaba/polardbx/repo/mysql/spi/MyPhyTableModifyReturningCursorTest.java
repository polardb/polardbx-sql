package com.alibaba.polardbx.repo.mysql.spi;

import com.alibaba.polardbx.common.logical.ITConnection;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.optimizer.biv.MockResultSetMetaData;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.BaseTableOperation;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MyPhyTableModifyReturningCursorTest {
    @Test
    public void test() throws SQLException, NoSuchFieldException, IllegalAccessException {
        MyRepository repo = mock(MyRepository.class);
        MyJdbcHandler handler = mock(MyJdbcHandler.class);
        when(handler.executeUpdate(any())).thenReturn(new int[] {1});
        ResultSet rs = mock(ResultSet.class);
        when(handler.getResultSet()).thenReturn(rs);
        MockResultSetMetaData meta = new MockResultSetMetaData(new ArrayList<>(), new HashMap<>());
        when(rs.getMetaData()).thenReturn(meta);
        ExecutionContext ec = new ExecutionContext();
        when(repo.createQueryHandler(ec)).thenReturn(handler);
        ITConnection connection = mock(ITConnection.class);
        ec.setConnection(connection);
        BaseTableOperation logicalPlan = mock(BaseTableOperation.class);
        ec.getParamManager().getProps().put(ConnectionProperties.SEQUENTIAL_CONCURRENT_POLICY, "true");

        MyPhyTableModifyReturningCursor cursor = new MyPhyTableModifyReturningCursor(ec, logicalPlan, repo,
            0, null, null);

        Field field = cursor.getClass().getSuperclass().getSuperclass().getSuperclass().getDeclaredField("inited");
        field.setAccessible(true);
        // Should delay init.
        Assert.assertFalse((Boolean) field.get(cursor));
        // Should execute in doInit.
        cursor.doInit();
        Assert.assertTrue((Boolean) field.get(cursor));
    }

    @Test
    public void test1() throws SQLException, NoSuchFieldException, IllegalAccessException {
        MyRepository repo = mock(MyRepository.class);
        MyJdbcHandler handler = mock(MyJdbcHandler.class);
        when(handler.executeUpdate(any())).thenReturn(new int[] {1});
        ResultSet rs = mock(ResultSet.class);
        when(handler.getResultSet()).thenReturn(rs);
        MockResultSetMetaData meta = new MockResultSetMetaData(new ArrayList<>(), new HashMap<>());
        when(rs.getMetaData()).thenReturn(meta);
        ExecutionContext ec = new ExecutionContext();
        when(repo.createQueryHandler(ec)).thenReturn(handler);
        ITConnection connection = mock(ITConnection.class);
        ec.setConnection(connection);
        BaseTableOperation logicalPlan = mock(BaseTableOperation.class);
        ec.getParamManager().getProps().put(ConnectionProperties.SEQUENTIAL_CONCURRENT_POLICY, "false");
        ec.getParamManager().getProps().put(ConnectionProperties.FIRST_THEN_CONCURRENT_POLICY, "true");
        MyPhyTableModifyReturningCursor cursor = new MyPhyTableModifyReturningCursor(ec, logicalPlan, repo,
            0, null, null);

        Field field = cursor.getClass().getSuperclass().getSuperclass().getSuperclass().getDeclaredField("inited");
        field.setAccessible(true);
        Assert.assertTrue((Boolean) field.get(cursor));
    }
}
