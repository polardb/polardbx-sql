/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.server;

import com.alibaba.polardbx.CobarConfig;
import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.thread.ServerThreadPool;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.matrix.jdbc.TConnection;
import com.alibaba.polardbx.net.AbstractConnection;
import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.net.NIOProcessor;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.compress.PacketOutputProxyFactory;
import com.alibaba.polardbx.net.handler.NIOHandler;
import com.alibaba.polardbx.net.packet.ErrorPacket;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.server.util.PacketUtil;
import com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ServerConnectionTest {

    private ServerConnection serverConnection;
    private Logger logger;

    @Before
    public void setUp() throws IOException {
        serverConnection = Mockito.spy(Mockito.mock(ServerConnection.class));
        logger = Mockito.mock(Logger.class);
    }

    @AfterClass
    public static void cleanUp() {
        DynamicConfig.getInstance().loadValue(null, "MAPPING_TO_MYSQL_ERROR_CODE", "");
    }

    @Test
    public void testLogError() {
        // check EOFException
        ErrorCode errCode = ErrorCode.ERR_HANDLE_DATA;
        Throwable t = new EOFException();
        String sql = "SELECT * FROM table";
        when(logger.isInfoEnabled()).thenReturn(true);
        when(logger.isDebugEnabled()).thenReturn(true);
        when(logger.isWarnEnabled()).thenReturn(true);

        // test process
        serverConnection.logError(logger, errCode, sql, t, null);

        // Assert buildMDC and logger.info
        verify(serverConnection, times(1)).buildMDC();
        verify(logger, times(1)).info((Throwable) any());

        // clear invocation
        Mockito.clearInvocations(serverConnection);
        Mockito.clearInvocations(logger);

        // check ClosedChannelException
        t = new ClosedChannelException();
        // test process
        serverConnection.logError(logger, errCode, sql, t, null);

        // Assert buildMDC and logger.info
        verify(serverConnection, times(1)).buildMDC();
        verify(logger, times(1)).info((Throwable) any());

        // clear invocation
        Mockito.clearInvocations(serverConnection);
        Mockito.clearInvocations(logger);

        // check isConnectionReset
        t = new IOException("Connection reset by peer");
        // test process
        serverConnection.logError(logger, errCode, sql, t, null);

        // Assert buildMDC and logger.info
        verify(serverConnection, times(1)).buildMDC();
        verify(logger, times(1)).info((Throwable) any());

        // clear invocation
        Mockito.clearInvocations(serverConnection);
        Mockito.clearInvocations(logger);

        // check Table doesn't exist
        t = new Exception("Table xx doesn't exist");
        // test process
        serverConnection.logError(logger, errCode, sql, t, null);

        // Assert buildMDC and logger.debug
        verify(serverConnection, times(1)).buildMDC();
        verify(logger, times(1)).debug((Throwable) any());

        // clear invocation
        Mockito.clearInvocations(serverConnection);
        Mockito.clearInvocations(logger);

        // check Column not found
        t = new Exception("Column xx not found in any table");
        // test process
        serverConnection.logError(logger, errCode, sql, t, null);

        // Assert buildMDC and logger.debug
        verify(serverConnection, times(1)).buildMDC();
        verify(logger, times(1)).debug((Throwable) any());

        // clear invocation
        Mockito.clearInvocations(serverConnection);
        Mockito.clearInvocations(logger);

        // check isMySQLIntegrityConstraintViolationException
        t = new MySQLIntegrityConstraintViolationException();
        // test process
        serverConnection.logError(logger, errCode, sql, t, null);

        // Assert buildMDC and logger.debug
        verify(serverConnection, times(1)).buildMDC();
        verify(logger, times(1)).debug((Throwable) any());

        // clear invocation
        Mockito.clearInvocations(serverConnection);
        Mockito.clearInvocations(logger);

        // check warning log
        t = new TddlRuntimeException(ErrorCode.ERR_READ);
        // test process
        serverConnection.logError(logger, errCode, sql, t, null);

        // Assert buildMDC and logger.debug
        verify(serverConnection, times(1)).buildMDC();
        verify(logger, times(1)).warn(anyString(), any());

        // clear invocation
        Mockito.clearInvocations(serverConnection);
        Mockito.clearInvocations(logger);
    }

    @Test
    public void testHandleError() throws NoSuchFieldException, IllegalAccessException {
        final ExecutionContext ec = new ExecutionContext();
        try (final MockedConstruction<ServerConnection> serverConnectionMockedConstruction =
            mockConstruction(ServerConnection.class, (mock, context) -> {
                doCallRealMethod().when(mock).handleError(any(), any(), anyString(), anyBoolean());
                doNothing().when(mock).writeErrMessage(anyInt(), any(), anyString());
            });
        ) {
            final ServerConnection serverConnection = new ServerConnection(null);
            // ServerConnection.conn=null
            serverConnection.handleError(ErrorCode.ERR_HANDLE_DATA,
                new TddlRuntimeException(ErrorCode.ERR_TABLE_NOT_EXIST),
                "show create table t1",
                false);

            final TConnection mockTConnection = mock(TConnection.class);
            when(mockTConnection.getExecutionContext()).thenReturn(ec);

            final Field connField = ServerConnection.class.getDeclaredField("conn");
            connField.setAccessible(true);
            connField.set(serverConnection, mockTConnection);

            // ServerConnection.conn.getExecutionContext().getParamManager()=null
            final ParamManager paramManager = ec.getParamManager();
            ec.setParamManager(null);
            serverConnection.handleError(ErrorCode.ERR_HANDLE_DATA,
                new TddlRuntimeException(ErrorCode.ERR_TABLE_NOT_EXIST),
                "show create table t1",
                false);
            ec.setParamManager(paramManager);

            // OUTPUT_MYSQL_ERROR_CODE=false
            serverConnection.handleError(ErrorCode.ERR_HANDLE_DATA,
                new TddlRuntimeException(ErrorCode.ERR_TABLE_NOT_EXIST),
                "show create table t1",
                false);

            // OUTPUT_MYSQL_ERROR_CODE=true
            ParamManager.setBooleanVal(ec.getParamManager().getProps(),
                ConnectionParams.OUTPUT_MYSQL_ERROR_CODE,
                true,
                false);
            serverConnection.handleError(ErrorCode.ERR_HANDLE_DATA,
                new TddlRuntimeException(ErrorCode.ERR_TABLE_NOT_EXIST),
                "show create table t1",
                false);

            // DynamicConfig.getInstance().getErrorCodeMapping is not empty
            DynamicConfig.getInstance().loadValue(null, "MAPPING_TO_MYSQL_ERROR_CODE", "{4007:1146}");
            serverConnection.handleError(ErrorCode.ERR_HANDLE_DATA,
                new TddlRuntimeException(ErrorCode.ERR_TABLE_NOT_EXIST),
                "show create table t1",
                false);

            // check mapping result
            DynamicConfig.getInstance().loadValue(null, "MAPPING_TO_MYSQL_ERROR_CODE", "{4006:1146}");
            serverConnection.handleError(ErrorCode.ERR_HANDLE_DATA,
                new TddlRuntimeException(ErrorCode.ERR_TABLE_NOT_EXIST),
                "show create table t1",
                false);

            verify(serverConnection, atMost(5)).writeErrMessage(eq(4006), isNull(), anyString());
            verify(serverConnection).writeErrMessage(eq(1146), isNull(), anyString());
        }
    }

    @Test
    public void testSet() {
        serverConnection.setUser("polardbx_root");
        Assert.assertTrue(serverConnection.isPolardbxRoot());
    }

    @Test
    public void testLock() {
        ConfigDataMode.Mode mode = ConfigDataMode.getMode();
        ConfigDataMode.setMode(ConfigDataMode.Mode.MOCK);
        try (MockedStatic<CobarServer> mockedStaticServer = Mockito.mockStatic(CobarServer.class);
            MockedStatic<PacketUtil> mockedUtil = Mockito.mockStatic(PacketUtil.class);
            MockedStatic<PacketOutputProxyFactory> mockedStaticPacketFactory = Mockito.mockStatic(
                PacketOutputProxyFactory.class)) {

            CobarServer mockedServer = mock(CobarServer.class);
            PacketOutputProxyFactory mockedPacketFactory = mock(PacketOutputProxyFactory.class);
            CobarConfig mockedConfig = mock(CobarConfig.class);

            mockedStaticServer.when(CobarServer::getInstance).thenReturn(mockedServer);
            mockedStaticPacketFactory.when(PacketOutputProxyFactory::getInstance).thenReturn(mockedPacketFactory);
            when(mockedServer.getConfig()).thenReturn(mockedConfig);
            IPacketOutputProxy proxy = mock(IPacketOutputProxy.class);
            when(mockedConfig.isLock()).thenReturn(true);

            ErrorPacket mockedPacket = mock(ErrorPacket.class);
            final AtomicInteger errCount = new AtomicInteger(0);

            doAnswer((invocation) -> {
                errCount.incrementAndGet();
                return proxy;
            }).when(mockedPacket).write(proxy);
            mockedUtil.when(PacketUtil::getLock).thenReturn(mockedPacket);

            doReturn(true).when(serverConnection).isClosed();

            serverConnection.setUser("testUser");
            Assert.assertFalse(serverConnection.isPolardbxRoot());

            ServerQueryHandler queryHandler = new ServerQueryHandler(serverConnection);
            serverConnection.setConnectionCharset("utf8");
            serverConnection.setQueryHandler(queryHandler);

            try {
                serverConnection.query(new byte[] {
                    33, 0, 0, 0, 3, 115, 101, 108, 101, 99, 116, 32, 64, 64, 118, 101, 114, 115, 105,
                    111, 110, 95, 99, 111, 109, 109, 101, 110, 116, 32, 108, 105, 109, 105, 116, 32, 49});
            } catch (Exception e) {
                Assert.fail(e.getMessage());
            }
        } finally {
            ConfigDataMode.setMode(mode);
        }
    }

    @Test
    public void testHandleDataThrow() throws Exception {
        Field field = FrontendConnection.class.getDeclaredField("executingFuture");
        field.setAccessible(true); // Make the private field accessible
        field.set(serverConnection, new AtomicReference<>(CompletableFuture.completedFuture(true)));

        final NIOProcessor processor = mock(NIOProcessor.class);
        final ServerThreadPool mockedPool = spy(new ServerThreadPool("test", 1, 1000, 1));
        when(processor.getHandler()).thenReturn(mockedPool);
        field = AbstractConnection.class.getDeclaredField("processor");
        field.setAccessible(true); // Make the private field accessible
        field.set(serverConnection, processor);

        final NIOHandler handler = mock(NIOHandler.class);
        field = FrontendConnection.class.getDeclaredField("handler");
        field.setAccessible(true); // Make the private field accessible
        field.set(serverConnection, handler);

        field = FrontendConnection.class.getDeclaredField("rescheduled");
        field.setAccessible(true); // Make the private field accessible
        field.set(serverConnection, true);

        field = FrontendConnection.class.getDeclaredField("packageLimit");
        field.setAccessible(true); // Make the private field accessible
        field.set(serverConnection, 16777215);

        field = AbstractConnection.class.getDeclaredField("isClosed");
        field.setAccessible(true); // Make the private field accessible
        field.set(serverConnection, new AtomicBoolean(true));

        final byte[] data0 = new byte[] {1, 0, 0, 0, 1};
        long cnt = mockedPool.getCompletedTaskCount();
        serverConnection.handleData(data0);
        while (mockedPool.getCompletedTaskCount() < cnt + 1) {
            Thread.sleep(1);
        }

        field = FrontendConnection.class.getDeclaredField("isBinlogDumpConn");
        field.setAccessible(true); // Make the private field accessible
        field.set(serverConnection, true);

        cnt = mockedPool.getCompletedTaskCount();
        serverConnection.handleData(data0);
        while (mockedPool.getCompletedTaskCount() < cnt + 1) {
            Thread.sleep(1);
        }

        field.set(serverConnection, false);

        final byte[] data1 = new byte[] {1, 0, 0, 0, 2};
        cnt = mockedPool.getCompletedTaskCount();
        serverConnection.handleData(data1);
        while (mockedPool.getCompletedTaskCount() < cnt + 1) {
            Thread.sleep(1);
        }

        field = FrontendConnection.class.getDeclaredField("rescheduled");
        field.setAccessible(true); // Make the private field accessible
        field.set(serverConnection, false);

        cnt = mockedPool.getCompletedTaskCount();
        serverConnection.handleData(data0);
        while (mockedPool.getCompletedTaskCount() < cnt + 1) {
            Thread.sleep(1);
        }

        field = FrontendConnection.class.getDeclaredField("executingFuture");
        field.setAccessible(true); // Make the private field accessible
        final Future<?> future = spy(CompletableFuture.completedFuture(true));
        field.set(serverConnection, new AtomicReference<>(future));

        when(future.get()).thenThrow(new RuntimeException("test"));

        cnt = mockedPool.getCompletedTaskCount();
        serverConnection.handleData(data0);
        while (mockedPool.getCompletedTaskCount() < cnt + 1) {
            Thread.sleep(1);
        }
    }
}
