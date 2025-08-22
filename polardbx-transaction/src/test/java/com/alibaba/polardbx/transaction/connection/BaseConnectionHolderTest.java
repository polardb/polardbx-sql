package com.alibaba.polardbx.transaction.connection;

import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.group.jdbc.TGroupDirectConnection;
import org.apache.calcite.util.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BaseConnectionHolderTest {
    private static class MockConnectionHolder extends BaseConnectionHolder {

        @Override
        public IConnection getConnection(String schema, String group, IDataSource ds) throws SQLException {
            return null;
        }

        @Override
        public void tryClose(IConnection conn, String groupName) throws SQLException {

        }

        public void addOneMockConnection(long id, String groupKey) {
            IConnection connection = mock(IConnection.class);
            TGroupDirectConnection tGroupDirectConnection = mock(TGroupDirectConnection.class);
            when(connection.getRealConnection()).thenReturn(tGroupDirectConnection);
            TGroupDataSource tGroupDataSource = mock(TGroupDataSource.class);
            when(tGroupDirectConnection.getGroupDataSource()).thenReturn(tGroupDataSource);
            when(tGroupDataSource.getDbGroupKey()).thenReturn(groupKey);
            when(tGroupDirectConnection.getId()).thenReturn(id);
            connections.add(connection);
        }
    }

    @Test
    public void test() {
        MockConnectionHolder holder = new MockConnectionHolder();
        holder.addOneMockConnection(0, "test0");
        holder.addOneMockConnection(0, "test0");
        List<Pair<Long, String>> results = new ArrayList<>();
        holder.handleConnIds((b, a) -> results.add(new Pair<>(a, b)));
        Assert.assertEquals(2, results.size());
        Assert.assertEquals(0L, Long.parseLong(results.get(0).left.toString()));
        Assert.assertEquals("test0", results.get(0).right);
        Assert.assertEquals(0L, Long.parseLong(results.get(1).left.toString()));
        Assert.assertEquals("test0", results.get(1).right);
    }
}
