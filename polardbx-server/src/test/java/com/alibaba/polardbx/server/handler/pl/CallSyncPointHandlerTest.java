package com.alibaba.polardbx.server.handler.pl;

import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.matrix.jdbc.TResultSet;
import com.alibaba.polardbx.optimizer.core.row.ArrayRow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.server.QueryResultHandler;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.handler.SyncPointExecutor;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class CallSyncPointHandlerTest {
    @Test
    public void test() throws Exception {
        try (MockedStatic<SyncPointExecutor> mocked = mockStatic(SyncPointExecutor.class)) {
            CallHandler handler = new CallHandler();
            SyncPointExecutor executor = mock(SyncPointExecutor.class);
            mocked.when(SyncPointExecutor::getInstance).thenReturn(executor);

            // Mock server connection.
            ServerConnection serverConnection = mock(ServerConnection.class);
            QueryResultHandler queryResultHandler = mock(QueryResultHandler.class);
            when(serverConnection.createResultHandler(false)).thenReturn(queryResultHandler);
            AtomicInteger flag = new AtomicInteger(0);
            doAnswer(
                invocation -> {
                    TResultSet resultSet = invocation.getArgument(0);
                    ArrayResultCursor cursor = (ArrayResultCursor) resultSet.getResultCursor();
                    List<Row> rows = cursor.getRows();
                    ArrayRow row = (ArrayRow) rows.get(0);
                    String result = (String) row.getObject(0);
                    if ("OK".equals(result)) {
                        flag.set(1);
                    } else if ("FAIL".equals(result)) {
                        flag.set(-1);
                    }
                    return null;
                }
            ).when(queryResultHandler).sendSelectResult(any(), any(), anyLong());
            doNothing().when(queryResultHandler).sendPacketEnd(false);

            when(executor.execute()).thenReturn(false);
            Assert.assertTrue(
                handler.handle(ByteString.from("call polardbx.trigger_sync_point_trx()"),
                    serverConnection, false));
            Assert.assertEquals(-1, flag.get());

            when(executor.execute()).thenReturn(true);
            Assert.assertTrue(
                handler.handle(ByteString.from("call polardbx.trigger_sync_point_trx()"),
                    serverConnection, false));
            Assert.assertEquals(1, flag.get());

            AtomicBoolean error = new AtomicBoolean(false);
            doAnswer(
                invocation -> {
                    error.set(true);
                    return null;
                }
            ).when(serverConnection).writeErrMessage(any(), any());
            doThrow(new SQLException("test")).when(queryResultHandler).sendSelectResult(any(), any(), anyLong());
            Assert.assertTrue(
                handler.handle(ByteString.from("call polardbx.trigger_sync_point_trx()"),
                    serverConnection, false));
            Assert.assertTrue(error.get());

            error.set(false);
            Assert.assertTrue(
                handler.handle(ByteString.from("call polardbx.no_such_procedure()"),
                    serverConnection, false));
            Assert.assertTrue(error.get());
        }
    }
}
