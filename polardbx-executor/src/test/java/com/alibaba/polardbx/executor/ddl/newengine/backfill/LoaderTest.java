package com.alibaba.polardbx.executor.ddl.newengine.backfill;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.backfill.Loader;
import com.alibaba.polardbx.executor.columns.ColumnBackfillExecutor;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.gsi.backfill.GsiLoader;
import com.alibaba.polardbx.executor.spi.ITransactionManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlInsert;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import javax.sql.DataSource;
import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

public class LoaderTest {

    @Test
    public void testCanUseBackfillReturning() {
        ExecutionContext ec = Mockito.mock(ExecutionContext.class);
        ParamManager pm = Mockito.mock(ParamManager.class);
        ExecutorContext executorContext = Mockito.mock(ExecutorContext.class);
        TopologyHandler topologyHandler = Mockito.mock(TopologyHandler.class);
        StorageInfoManager sm = Mockito.mock(StorageInfoManager.class);

        when(ec.getParamManager()).thenReturn(pm);
        when(pm.getBoolean(Mockito.any())).thenReturn(true);

        when(executorContext.getTopologyHandler()).thenReturn(topologyHandler);
        when(executorContext.getStorageInfoManager()).thenReturn(sm);
        when(sm.supportsBackfillReturning()).thenReturn(true);

        try (MockedStatic<ExecutorContext> executorContextMockedStatic = Mockito.mockStatic(ExecutorContext.class)) {
            when(ExecutorContext.getContext(Mockito.anyString())).thenReturn(executorContext);

            try (MockedStatic<ColumnBackfillExecutor> mockedStatic = Mockito.mockStatic(ColumnBackfillExecutor.class)) {
                when(ColumnBackfillExecutor.isAllDnUseXDataSource(Mockito.any(TopologyHandler.class)))
                    .thenReturn(true);

                Assert.assertTrue(Loader.canUseBackfillReturning(ec, "wumu"));
            }
        }
    }

    @Test
    public void testFillIntoIndex() throws Exception {
        Class loaderClass = Class.forName("com.alibaba.polardbx.executor.gsi.backfill.GsiLoader");
        Constructor[] constructors = loaderClass.getDeclaredConstructors();
        Constructor protectedConstructor = constructors[0];
        protectedConstructor.setAccessible(true);

        BiFunction<List<RelNode>, ExecutionContext, List<Cursor>> executeFunc =
            (List<RelNode> inputs, ExecutionContext executionContext1) -> {
                List<Cursor> inputCursors = new ArrayList<>(inputs.size());
                return inputCursors;
            };

        ExecutionContext ec = Mockito.mock(ExecutionContext.class);
        ITransactionManager tm = Mockito.mock(ITransactionManager.class);
        ExecutorContext executorContext = Mockito.mock(ExecutorContext.class);
        SqlInsert sqlInsert = Mockito.mock(SqlInsert.class);
        ExecutionPlan executionPlan = Mockito.mock(ExecutionPlan.class);
        ITransaction transaction = Mockito.mock(ITransaction.class);

        when(ec.getBackfillReturning()).thenReturn(null);
        when(ec.copy()).thenReturn(ec);
        when(ec.isReadOnly()).thenReturn(false);
        when(ec.getTransaction()).thenReturn(transaction);
        when(tm.createTransaction(Mockito.any(ITransactionPolicy.TransactionClass.class),
            Mockito.any(ExecutionContext.class))).thenReturn(transaction);
        when(executorContext.getTransactionManager()).thenReturn(tm);

        List<Map<Integer, ParameterContext>> batchParams = new ArrayList<>();
        Map<Integer, ParameterContext> param = new HashMap<>();
        batchParams.add(param);

        Pair<ExecutionContext, Pair<String, String>> baseEcAndIndexPair = new Pair<>(ec, new Pair<>("wumu", "t1"));

        try (MockedStatic<ExecutorContext> executorContextMockedStatic = Mockito.mockStatic(ExecutorContext.class)) {
            when(ExecutorContext.getContext(Mockito.anyString())).thenReturn(executorContext);

            Object[] constructorArgs =
                new Object[] {
                    "wumu", "t1", sqlInsert, sqlInsert, executionPlan, new int[2], new int[2], executeFunc, "a"};
            Object loaderObject = protectedConstructor.newInstance(constructorArgs);

            GsiLoader loader = Mockito.spy((GsiLoader) loaderObject);
            doReturn(1024).when(loader)
                .executeInsert(Mockito.any(SqlInsert.class), Mockito.anyString(), Mockito.anyString(),
                    Mockito.any(ExecutionContext.class), Mockito.anyString(), Mockito.anyString());

            Assert.assertEquals(1024, loader.fillIntoIndex(batchParams, baseEcAndIndexPair, () -> true));
        }
    }

    @Test
    public void testFillIntoIndexErr() throws Exception {
        Class loaderClass = Class.forName("com.alibaba.polardbx.executor.gsi.backfill.GsiLoader");
        Constructor[] constructors = loaderClass.getDeclaredConstructors();
        Constructor protectedConstructor = constructors[0];
        protectedConstructor.setAccessible(true);

        BiFunction<List<RelNode>, ExecutionContext, List<Cursor>> executeFunc =
            (List<RelNode> inputs, ExecutionContext executionContext1) -> {
                List<Cursor> inputCursors = new ArrayList<>(inputs.size());
                return inputCursors;
            };

        ExecutionContext ec = Mockito.mock(ExecutionContext.class);
        ITransactionManager tm = Mockito.mock(ITransactionManager.class);
        ExecutorContext executorContext = Mockito.mock(ExecutorContext.class);
        SqlInsert sqlInsert = Mockito.mock(SqlInsert.class);
        ExecutionPlan executionPlan = Mockito.mock(ExecutionPlan.class);
        ITransaction transaction = Mockito.mock(ITransaction.class);

        when(ec.getBackfillReturning()).thenReturn(null);
        when(ec.copy()).thenReturn(ec);
        when(ec.isReadOnly()).thenReturn(false);
        when(ec.getTransaction()).thenReturn(transaction);
        when(tm.createTransaction(Mockito.any(ITransactionPolicy.TransactionClass.class),
            Mockito.any(ExecutionContext.class))).thenReturn(transaction);
        when(executorContext.getTransactionManager()).thenReturn(tm);

        List<Map<Integer, ParameterContext>> batchParams = new ArrayList<>();
        Map<Integer, ParameterContext> param = new HashMap<>();
        batchParams.add(param);

        Pair<ExecutionContext, Pair<String, String>> baseEcAndIndexPair = new Pair<>(ec, new Pair<>("wumu", "t1"));

        try (MockedStatic<ExecutorContext> executorContextMockedStatic = Mockito.mockStatic(ExecutorContext.class)) {
            when(ExecutorContext.getContext(Mockito.anyString())).thenReturn(executorContext);

            Object[] constructorArgs =
                new Object[] {
                    "wumu", "t1", sqlInsert, sqlInsert, executionPlan, new int[2], new int[2], executeFunc, "a"};
            Object loaderObject = protectedConstructor.newInstance(constructorArgs);

            GsiLoader loader = Mockito.spy((GsiLoader) loaderObject);
            doThrow(new TddlRuntimeException(ErrorCode.ERR_GLOBAL_SECONDARY_INDEX_BACKFILL_DUPLICATE_ENTRY, "error"))
                .when(loader)
                .executeInsert(Mockito.any(SqlInsert.class), Mockito.anyString(), Mockito.anyString(),
                    Mockito.any(ExecutionContext.class), Mockito.anyString(), Mockito.anyString());

            try {
                loader.fillIntoIndex(batchParams, baseEcAndIndexPair, () -> true);
            } catch (TddlRuntimeException e) {
                Assert.assertTrue(e.getMessage().contains("error"));
            }
        }
    }

    @Test
    public void testCheckSupportBackfillReturning() throws SQLException {
        DataSource dataSource = Mockito.mock(DataSource.class);
        Connection connection = Mockito.mock(Connection.class);
        Statement statement = Mockito.mock(Statement.class);
        ResultSet resultSet = Mockito.mock(ResultSet.class);

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(Mockito.anyString())).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getString(1)).thenReturn("dbms_trans");
        when(resultSet.getString(2)).thenReturn("backfill");

        try (MockedStatic<ConfigDataMode> configDataModeMockedStatic = Mockito.mockStatic(ConfigDataMode.class)) {
            when(ConfigDataMode.isPolarDbX()).thenReturn(true);

            Assert.assertTrue(StorageInfoManager.checkSupportBackfillReturning(dataSource));
        }
    }

    @Test
    public void testCheckSupportBackfillReturningErr() throws SQLException {
        DataSource dataSource = Mockito.mock(DataSource.class);
        Connection connection = Mockito.mock(Connection.class);
        Statement statement = Mockito.mock(Statement.class);

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(Mockito.anyString())).thenThrow(
            new SQLException("Command not supported by pluggable protocols", "HY000", 3130));

        try (MockedStatic<ConfigDataMode> configDataModeMockedStatic = Mockito.mockStatic(ConfigDataMode.class)) {
            when(ConfigDataMode.isPolarDbX()).thenReturn(true);

            Assert.assertFalse(StorageInfoManager.checkSupportBackfillReturning(dataSource));
        }
    }

    @Test
    public void testCheckSupportBackfillReturningErr2() throws SQLException {
        DataSource dataSource = Mockito.mock(DataSource.class);
        Connection connection = Mockito.mock(Connection.class);
        Statement statement = Mockito.mock(Statement.class);

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(Mockito.anyString())).thenThrow(
            new SQLException("does not exist", "42000", 1305));

        try (MockedStatic<ConfigDataMode> configDataModeMockedStatic = Mockito.mockStatic(ConfigDataMode.class)) {
            when(ConfigDataMode.isPolarDbX()).thenReturn(true);

            Assert.assertFalse(StorageInfoManager.checkSupportBackfillReturning(dataSource));
        }
    }

    @Test
    public void testBackfillReturningLoader() {
        ExecutionContext ec = Mockito.mock(ExecutionContext.class);

        Parameters parameters = Mockito.mock(Parameters.class);

        List<Map<Integer, ParameterContext>> params = new ArrayList<>();
        params.add(new HashMap<>());
        params.add(new HashMap<>());

        when(parameters.getBatchParameters()).thenReturn(params);

        when(ec.getParams()).thenReturn(parameters);

        List<Map<Integer, ParameterContext>> returningResult = new ArrayList<>();

        Assert.assertEquals(Loader.getReturningAffectRows(returningResult, ec), 2);
    }

    @Test
    public void testBackfillReturningLoader2() {
        ExecutionContext ec = Mockito.mock(ExecutionContext.class);

        Parameters parameters = Mockito.mock(Parameters.class);

        List<Map<Integer, ParameterContext>> params = new ArrayList<>();
        params.add(new HashMap<>());
        params.add(new HashMap<>());

        when(parameters.getBatchParameters()).thenReturn(params);

        when(ec.getParams()).thenReturn(parameters);

        List<Map<Integer, ParameterContext>> returningResult = new ArrayList<>();
        Map<Integer, ParameterContext> res = new HashMap<>();
        ParameterContext pc = Mockito.mock(ParameterContext.class);
        res.put(1, pc);
        returningResult.add(res);

        Object[] objects = new Object[2];
        objects[0] = 1;
        objects[1] = "wumu";

        when(pc.getArgs()).thenReturn(objects);

        try {
            Loader.getReturningAffectRows(returningResult, ec);
        } catch (TddlRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("Duplicated entry 'wumu' for key 'PRIMARY'"));
            return;
        }

        Assert.fail();
    }
}
