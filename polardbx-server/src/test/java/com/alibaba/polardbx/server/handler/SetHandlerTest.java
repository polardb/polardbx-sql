package com.alibaba.polardbx.server.handler;

import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.cdc.ICdcManager;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.InstanceRole;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.server.ServerConnection;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class SetHandlerTest {
    @Test
    public void test0() {
        ServerConnection serverConnection = mock(ServerConnection.class);
        Map<String, Object> map = new HashMap<>();
        map.put(ConnectionProperties.IGNORE_TRANSACTION_POLICY_NO_TRANSACTION, "true");
        when(serverConnection.getConnectionVariables()).thenReturn(map);
        Assert.assertTrue(SetHandler.isIgnoreSettingNoTransaction(serverConnection));
    }

    @Test
    public void test1() {
        ServerConnection serverConnection = mock(ServerConnection.class);
        Map<String, Object> map = new HashMap<>();
        when(serverConnection.getConnectionVariables()).thenReturn(map);
        ExecutionContext ec = new ExecutionContext();
        when(serverConnection.getExecutionContext()).thenReturn(ec);
        try (MockedStatic<InstConfUtil> mockedStatic = mockStatic(InstConfUtil.class)) {
            mockedStatic.when(() -> InstConfUtil.getBool(ConnectionParams.IGNORE_TRANSACTION_POLICY_NO_TRANSACTION))
                .thenReturn(true);
            Assert.assertTrue(SetHandler.isIgnoreSettingNoTransaction(serverConnection));
        }
    }

    @Test
    public void test2() {
        ServerConnection serverConnection = mock(ServerConnection.class);
        Map<String, Object> map = new HashMap<>();
        when(serverConnection.getConnectionVariables()).thenReturn(map);
        ExecutionContext ec = new ExecutionContext();
        when(serverConnection.getExecutionContext()).thenReturn(ec);
        Map<String, String> props = new HashMap<>();
        props.put(ConnectionProperties.IGNORE_TRANSACTION_POLICY_NO_TRANSACTION, "true");
        ParamManager paramManager = new ParamManager(props);
        ec.setParamManager(paramManager);
        Assert.assertTrue(SetHandler.isIgnoreSettingNoTransaction(serverConnection));
    }

    @Test
    public void setSqlLogBinXTest() throws NoSuchFieldException, IllegalAccessException {
        try {
            ServerConnection serverConnection = mock(ServerConnection.class);
            when(serverConnection.initOptimizerContext()).thenReturn(true);
            ByteBuffer buffer = ByteBuffer.allocate(1024);
            ByteBufferHolder holder = new ByteBufferHolder(buffer);
            when(serverConnection.allocate()).thenReturn(holder);
            when(serverConnection.checkWriteBuffer(holder, 1)).thenReturn(holder);
            HashMap<String, Object> extraServerVariables = new HashMap<>();
            when(serverConnection.getExtraServerVariables()).thenReturn(extraServerVariables);
            ByteString byteString = new ByteString("set sql_log_bin=off".getBytes(), Charset.defaultCharset());
            ConfigDataMode.setInstanceRole(InstanceRole.FAST_MOCK);
            SetHandler.handleV2(byteString, serverConnection, 0, false);
            Assert.assertEquals(false, serverConnection.getExtraServerVariables().get(ICdcManager.SQL_LOG_BIN));

            byteString = new ByteString("set sql_log_bin=on".getBytes(), Charset.defaultCharset());
            ConfigDataMode.setInstanceRole(InstanceRole.FAST_MOCK);
            SetHandler.handleV2(byteString, serverConnection, 0, false);
            Assert.assertEquals(true, serverConnection.getExtraServerVariables().get(ICdcManager.SQL_LOG_BIN));
        } finally {
            ConfigDataMode.setInstanceRole(InstanceRole.MASTER);
        }

    }

    @Test
    public void strictSetGlobalTest() {

        try (MockedStatic<InstConfUtil> instUtilMockedStatic = Mockito.mockStatic(InstConfUtil.class)) {

            when(InstConfUtil.getValBool(TddlConstants.ENABLE_STRICT_SET_GLOBAL)).thenReturn(true);
            ServerConnection serverConnection = mock(ServerConnection.class);
            AtomicReference<String> msgRef = new AtomicReference<>(null);
            Mockito.doAnswer((invocation) -> {
                String msg = invocation.getArgument(1, String.class);
                msgRef.set(msg);
                return null;
            }).when(serverConnection).writeErrMessage(any(ErrorCode.class), anyString());

            List list = new ArrayList();
            SetHandler.handleGlobalVariable(serverConnection, list, list, list);

            Assert.assertNotNull(msgRef.get());
            Assert.assertTrue(msgRef.get(), msgRef.get().contains("is not allowed"));

        }
    }
}
