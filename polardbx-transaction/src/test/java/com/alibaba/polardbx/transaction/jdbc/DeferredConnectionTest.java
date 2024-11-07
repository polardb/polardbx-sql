package com.alibaba.polardbx.transaction.jdbc;

import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.group.jdbc.TGroupDirectConnection;
import com.alibaba.polardbx.rpc.pool.XConnection;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DeferredConnectionTest {
    @Test
    public void testFlashbackArea() throws SQLException {
        XConnection xConn = mock(XConnection.class);
        TGroupDirectConnection tGroupDirectConnection = mock(TGroupDirectConnection.class);
        when(tGroupDirectConnection.unwrap(XConnection.class)).thenReturn(xConn);
        when(tGroupDirectConnection.isWrapperFor(XConnection.class)).thenReturn(true);
        when(tGroupDirectConnection.enableFlashbackArea(anyBoolean())).thenCallRealMethod();
        DeferredConnection deferredConnection = new DeferredConnection(tGroupDirectConnection, false);
        when(deferredConnection.unwrap(XConnection.class)).thenReturn(xConn);
        when(xConn.supportFlashbackArea()).thenReturn(true);
        doNothing().when(xConn).setLazyFlashbackArea();

        IConnection result = deferredConnection.enableFlashbackArea(true);

        assertEquals(deferredConnection, result);
        verify(xConn).setLazyFlashbackArea();

        IConnection iConnection = mock(IConnection.class);
        doCallRealMethod().when(iConnection).enableFlashbackArea(true);
        doCallRealMethod().when(iConnection).disableFlashbackArea();
        when(tGroupDirectConnection.isWrapperFor(XConnection.class)).thenReturn(false);
        deferredConnection = new DeferredConnection(iConnection, false);

        result = deferredConnection.enableFlashbackArea(true);

        assertEquals(deferredConnection, result);
        verify(iConnection).executeLater(any());

        Statement stmt = mock(Statement.class);
        when(iConnection.createStatement()).thenReturn(stmt);
        deferredConnection.disableFlashbackArea();
        verify(stmt).execute(any());
    }
}
