package com.alibaba.polardbx.server.handler;

import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.cdc.ICdcManager;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.InstanceRole;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.server.ServerConnection;
import lombok.Data;
import org.apache.calcite.sql.SqlNode;
import org.hamcrest.Matchers;
import org.jetbrains.annotations.NotNull;
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
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import static com.alibaba.polardbx.common.jdbc.ITransactionPolicy.ALLOW_READ_CROSS_DB;
import static com.alibaba.polardbx.common.jdbc.ITransactionPolicy.ARCHIVE;
import static com.alibaba.polardbx.common.jdbc.ITransactionPolicy.BEST_EFFORT;
import static com.alibaba.polardbx.common.jdbc.ITransactionPolicy.FREE;
import static com.alibaba.polardbx.common.jdbc.ITransactionPolicy.IGNORE_BINLOG_TRANSACTION;
import static com.alibaba.polardbx.common.jdbc.ITransactionPolicy.NO_TRANSACTION;
import static com.alibaba.polardbx.common.jdbc.ITransactionPolicy.TSO;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SetHandlerTest {
    @Test
    public void test0() {
        ServerConnection serverConnection = mock(ServerConnection.class);
        Map<String, Object> map = new HashMap<>();
        map.put(ConnectionProperties.IGNORE_TRANSACTION_POLICY_NO_TRANSACTION, "true");
        when(serverConnection.getConnectionVariables()).thenReturn(map);
        Assert.assertTrue(SetHandler.isIgnoreSettingNoTransaction(4, serverConnection));
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
            Assert.assertTrue(SetHandler.isIgnoreSettingNoTransaction(4, serverConnection));
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
        Assert.assertTrue(SetHandler.isIgnoreSettingNoTransaction(4, serverConnection));
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

    @Test
    public void setTransactionPolicyTest() {
        final ServerConnection serverConnection = mockServerConnection();
        try {
            ConfigDataMode.setInstanceRole(InstanceRole.FAST_MOCK);

            checkSetTransactionPolicyResult("drds_transaction_policy", (v, k) -> String.format("set %s=%s", k, v),
                serverConnection);
            checkSetTransactionPolicyResult("trans.policy", (v, k) -> String.format("set %s=%s", k, v),
                serverConnection);
            checkSetTransactionPolicyResult("transaction policy", (v, k) -> String.format("set %s %s", k, v),
                serverConnection);
            checkSetTransactionPolicyResult("\"transaction policy\"", (v, k) -> String.format("set %s=%s", k, v),
                serverConnection);

        } finally {
            ConfigDataMode.setInstanceRole(InstanceRole.MASTER);
        }
    }

    private static @NotNull ServerConnection mockServerConnection() {
        final ServerConnection serverConnection = mock(ServerConnection.class);
        when(serverConnection.initOptimizerContext()).thenReturn(true);
        when(serverConnection.getVarStringValue(any(SqlNode.class))).thenCallRealMethod();
        when(serverConnection.getVarIntegerValue(any())).thenCallRealMethod();
        Mockito.doCallRealMethod().when(serverConnection).setTrxPolicy(any(ITransactionPolicy.class));
        when(serverConnection.getTrxPolicy()).thenCallRealMethod();
        doCallRealMethod().when(serverConnection).writeErrMessage(any(ErrorCode.class), anyString());
        final Map<String, Object> connectionVariables = new HashMap<>();
        when(serverConnection.getConnectionVariables()).thenReturn(connectionVariables);
        connectionVariables.put(ConnectionProperties.IGNORE_TRANSACTION_POLICY_NO_TRANSACTION, "false");
        final ByteBuffer buffer = ByteBuffer.allocate(1024);
        final ByteBufferHolder holder = new ByteBufferHolder(buffer);
        when(serverConnection.allocate()).thenReturn(holder);
        when(serverConnection.checkWriteBuffer(holder, 1)).thenReturn(holder);
        final HashMap<String, Object> extraServerVariables = new HashMap<>();
        when(serverConnection.getExtraServerVariables()).thenReturn(extraServerVariables);
        return serverConnection;
    }

    @Data
    private static final class SetTransactionPolicyResult {
        // ServerConnection::trxPolicy
        final ITransactionPolicy trxPolicy;
        // TRANSACTION POLICY
        final int intPolicy;
        // TRANS.POLICY / DRDS_TRANSACTION_POLICY
        final String stringPolicy;

        final String setStatement;
    }

    private static SetTransactionPolicyResult expectedSetTransactionPolicyResult(String key,
                                                                                 String valueStringPolicy,
                                                                                 int valueIntPolicy,
                                                                                 String currentStringPolicy,
                                                                                 Integer currentIntPolicy,
                                                                                 BiFunction<String, String, String> setGenerator) {
        // SET TRANS.POLICY/DRDS_TRANSACTION_POLICY = TSO
        final boolean stringPolicy = !TStringUtil.containsIgnoreCase(key, "transaction policy");
        // SET TRANSACTION POLICY 4
        final boolean setTransactionPolicy = TStringUtil.equalsIgnoreCase(key, "transaction policy");
        // SET "TRANSACTION POLICY" = 4
        final boolean setTransactionBlankPolicy = TStringUtil.equalsIgnoreCase(key, "\"transaction policy\"");

        if (setTransactionPolicy) {
            // set with int policy
            // set ServerConnection::trxPolicy only
            final ITransactionPolicy trxPolicy = ITransactionPolicy.of(valueIntPolicy);
            return new SetTransactionPolicyResult(trxPolicy,
                currentIntPolicy,
                currentStringPolicy,
                setGenerator.apply(String.valueOf(valueIntPolicy), key));
        } else if (setTransactionBlankPolicy) {
            // set with int policy
            // set ServerConnection::trxPolicy, TRANSACTION POLICY, TRANS.POLICY, DRDS_TRANSACTION_POLICY
            final ITransactionPolicy trxPolicy = ITransactionPolicy.of(valueIntPolicy);
            return new SetTransactionPolicyResult(trxPolicy,
                valueIntPolicy, // set to origin int policy value
                trxPolicy.toString(),
                setGenerator.apply(String.valueOf(valueIntPolicy), key));
        } else if (stringPolicy) {
            // set with string policy
            // set ServerConnection::trxPolicy, TRANSACTION POLICY, TRANS.POLICY, DRDS_TRANSACTION_POLICY
            final ITransactionPolicy trxPolicy = ITransactionPolicy.of(valueStringPolicy);
            return new SetTransactionPolicyResult(trxPolicy,
                trxPolicy.getIntPolicy(),
                valueStringPolicy.toUpperCase(),
                setGenerator.apply(valueStringPolicy, key));
        }

        throw new IllegalArgumentException("Unexpected set with key: " + key);
    }

    private static void checkSetTransactionPolicyResult(String key,
                                                        BiFunction<String, String, String> setGenerator,
                                                        ServerConnection serverConnection) {
        // SET TRANS.POLICY/DRDS_TRANSACTION_POLICY = TSO
        final boolean stringPolicy = !TStringUtil.containsIgnoreCase(key, "transaction policy");
        // SET "TRANSACTION POLICY" = 4
        final boolean setTransactionBlankPolicy = TStringUtil.equalsIgnoreCase(key, "\"transaction policy\"");

        setAncCheckError(setGenerator.apply(stringPolicy ? "TDDL" : "10086", key),
            setTransactionBlankPolicy ? "transaction policy" : key);

        final Integer currentIntPolicy =
            Optional.ofNullable(serverConnection.getExtraServerVariables().get("TRANSACTION POLICY".toLowerCase()))
                .map(v -> Integer.parseInt(v.toString())).orElse(-1);
        final String currentTransDotPolicy =
            Optional.ofNullable(serverConnection.getExtraServerVariables().get("TRANS.POLICY".toLowerCase()))
                .map(Object::toString).orElse("");

        // IGNORE_TRANSACTION_POLICY_NO_TRANSACTION = true only works for set transaction policy 4
        serverConnection.getConnectionVariables()
            .put(ConnectionProperties.IGNORE_TRANSACTION_POLICY_NO_TRANSACTION, "true");
        String valueStringPolicy = "BEST_EFFORT";
        int valueIntPolicy = BEST_EFFORT.getIntPolicy();
        setAndCheckTransactionPolicyResult(expectedSetTransactionPolicyResult(key,
                valueStringPolicy,
                valueIntPolicy,
                currentTransDotPolicy,
                currentIntPolicy,
                setGenerator),
            serverConnection);

        valueStringPolicy = "2PC";
        valueIntPolicy = BEST_EFFORT.getIntPolicy();
        setAndCheckTransactionPolicyResult(expectedSetTransactionPolicyResult(key,
                valueStringPolicy,
                valueIntPolicy,
                currentTransDotPolicy,
                currentIntPolicy,
                setGenerator),
            serverConnection);

        valueStringPolicy = "FLEXIBLE";
        valueIntPolicy = 2;
        setAndCheckTransactionPolicyResult(expectedSetTransactionPolicyResult(key,
                valueStringPolicy,
                valueIntPolicy,
                currentTransDotPolicy,
                currentIntPolicy,
                setGenerator),
            serverConnection);

        valueStringPolicy = "TSO";
        valueIntPolicy = TSO.getIntPolicy();
        setAndCheckTransactionPolicyResult(expectedSetTransactionPolicyResult(key,
                valueStringPolicy,
                valueIntPolicy,
                currentTransDotPolicy,
                currentIntPolicy,
                setGenerator),
            serverConnection);

        valueStringPolicy = "FREE";
        valueIntPolicy = FREE.getIntPolicy();
        setAndCheckTransactionPolicyResult(expectedSetTransactionPolicyResult(key,
                valueStringPolicy,
                valueIntPolicy,
                currentTransDotPolicy,
                currentIntPolicy,
                setGenerator),
            serverConnection);

        valueStringPolicy = "ALLOW_READ_CROSS_DB";
        valueIntPolicy = ALLOW_READ_CROSS_DB.getIntPolicy();
        setAndCheckTransactionPolicyResult(expectedSetTransactionPolicyResult(key,
                valueStringPolicy,
                valueIntPolicy,
                currentTransDotPolicy,
                currentIntPolicy,
                setGenerator),
            serverConnection);

        valueStringPolicy = "ALLOW_READ";
        valueIntPolicy = ALLOW_READ_CROSS_DB.getIntPolicy();
        setAndCheckTransactionPolicyResult(expectedSetTransactionPolicyResult(key,
                valueStringPolicy,
                valueIntPolicy,
                currentTransDotPolicy,
                currentIntPolicy,
                setGenerator),
            serverConnection);

        // IGNORE_TRANSACTION_POLICY_NO_TRANSACTION = true only works for set transaction policy 4
        serverConnection.getConnectionVariables()
            .put(ConnectionProperties.IGNORE_TRANSACTION_POLICY_NO_TRANSACTION, "true");
        if (stringPolicy) {
            valueStringPolicy = "NO_TRANSACTION";
            valueIntPolicy = NO_TRANSACTION.getIntPolicy();
        }
        setAndCheckTransactionPolicyResult(expectedSetTransactionPolicyResult(key,
                valueStringPolicy,
                valueIntPolicy,
                currentTransDotPolicy,
                currentIntPolicy,
                setGenerator),
            serverConnection);

        serverConnection.getConnectionVariables()
            .put(ConnectionProperties.IGNORE_TRANSACTION_POLICY_NO_TRANSACTION, "false");
        valueStringPolicy = "NO_TRANSACTION";
        valueIntPolicy = NO_TRANSACTION.getIntPolicy();
        setAndCheckTransactionPolicyResult(expectedSetTransactionPolicyResult(key,
                valueStringPolicy,
                valueIntPolicy,
                currentTransDotPolicy,
                currentIntPolicy,
                setGenerator),
            serverConnection);

        valueStringPolicy = "ARCHIVE";
        valueIntPolicy = ARCHIVE.getIntPolicy();
        setAndCheckTransactionPolicyResult(expectedSetTransactionPolicyResult(key,
                valueStringPolicy,
                valueIntPolicy,
                currentTransDotPolicy,
                currentIntPolicy,
                setGenerator),
            serverConnection);

        valueStringPolicy = "IGNORE_BINLOG_TRANSACTION";
        valueIntPolicy = IGNORE_BINLOG_TRANSACTION.getIntPolicy();
        setAndCheckTransactionPolicyResult(expectedSetTransactionPolicyResult(key,
                valueStringPolicy,
                valueIntPolicy,
                currentTransDotPolicy,
                currentIntPolicy,
                setGenerator),
            serverConnection);
    }

    private static void setAncCheckError(String setSql, String errKey) {
        final ServerConnection serverConn = mockServerConnection();

        final ByteString sql = ByteString.from(setSql);
        SetHandler.handleV2(sql, serverConn, 0, false, false);

        verify(serverConn).writeErrMessage(eq(ErrorCode.ER_WRONG_VALUE_FOR_VAR), contains(errKey));
    }

    private static void setAndCheckTransactionPolicyResult(SetTransactionPolicyResult setResult,
                                                           ServerConnection serverConnection) {
        final ByteString sql = ByteString.from(setResult.getSetStatement());
        SetHandler.handleV2(sql, serverConnection, 0, false);

        Assert.assertThat(sql + " : ServerConnection::trxPolicy",
            serverConnection.getTrxPolicy(),
            Matchers.is(setResult.getTrxPolicy()));

        Assert.assertThat(sql + " : transaction policy",
            serverConnection.getExtraServerVariables().get("transaction policy"),
            Matchers.is(setResult.getIntPolicy()));

        Assert.assertThat(sql + " : trans.policy",
            serverConnection.getExtraServerVariables().get("trans.policy").toString(),
            Matchers.is(setResult.getStringPolicy()));

        Assert.assertThat(sql + " : drds_transaction_policy",
            serverConnection.getExtraServerVariables().get("drds_transaction_policy").toString(),
            Matchers.is(setResult.getStringPolicy()));
    }
}
