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
package com.alibaba.polardbx.matrix.jdbc;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.ConcurrentHashSet;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.TddlGroupExecutor;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.group.config.GroupDataSourceHolder;
import com.alibaba.polardbx.group.config.OptimizedGroupConfigManager;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.group.jdbc.TGroupDirectConnection;
import com.alibaba.polardbx.optimizer.ccl.exception.CclRescheduleException;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.utils.ExecutionPlanProperties;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import com.alibaba.polardbx.rpc.compatible.XDataSource;
import com.alibaba.polardbx.rpc.perf.SwitchoverPerfCollection;
import com.alibaba.polardbx.rpc.pool.XConnectionManager;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.transaction.connection.AutoCommitConnectionHolder;
import com.alibaba.polardbx.transaction.connection.TransactionConnectionHolder;
import com.alibaba.polardbx.transaction.trx.AutoCommitTransaction;
import com.alibaba.polardbx.transaction.trx.TsoTransaction;
import com.google.common.collect.ImmutableSet;
import com.google.common.truth.Truth;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class TConnectionTest {
    @Test
    public void newExecutionContextTest() {

        final TConnection tConnection = new TConnection(new TDataSource());

        // OUTPUT_MYSQL_ERROR_CODE = false
        tConnection.newExecutionContext();
        Truth.assertThat(
                tConnection
                    .getExecutionContext()
                    .getParamManager()
                    .getBoolean(ConnectionParams.OUTPUT_MYSQL_ERROR_CODE))
            .isFalse();

        // OUTPUT_MYSQL_ERROR_CODE = true
        ParamManager.setBooleanVal(tConnection.getExecutionContext().getParamManager().getProps(),
            ConnectionParams.OUTPUT_MYSQL_ERROR_CODE, true, false);
        tConnection.newExecutionContext();
        Truth.assertThat(
                tConnection
                    .getExecutionContext()
                    .getParamManager()
                    .getBoolean(ConnectionParams.OUTPUT_MYSQL_ERROR_CODE))
            .isTrue();
    }

    @Test
    public void testUpdateTransactionAndConcurrentPolicyForDml() throws SQLException {
        try (final TConnection tConnection = mock(TConnection.class)) {
            ExecutionPlan plan = mock(ExecutionPlan.class);
            ExecutionContext ec;

            ConcurrentHashSet<Integer> properties = new ConcurrentHashSet<>();
            when(plan.getPlanProperties()).thenReturn(properties);
            SqlNode ast = mock(SqlNode.class);
            when(plan.getAst()).thenReturn(ast);
            when(plan.is(any())).thenCallRealMethod();
            ITransaction trx = mock(ITransaction.class);
            when(tConnection.forceInitTransaction(any(), anyBoolean(), anyBoolean())).thenReturn(trx);

            properties.clear();
            properties.add(ExecutionPlanProperties.DML);
            when(tConnection.isAutoCommit()).thenReturn(true);
            when(ast.getKind()).thenReturn(SqlKind.SELECT);
            ec = new ExecutionContext();
            ec.getParamManager().getProps().put(ConnectionProperties.FORBID_AUTO_COMMIT_TRX, "true");
            when(tConnection.updateTransactionAndConcurrentPolicyForDml(plan, ec)).thenCallRealMethod();

            boolean updated = tConnection.updateTransactionAndConcurrentPolicyForDml(plan, ec);
            Assert.assertTrue(updated);
            Assert.assertEquals(trx, ec.getTransaction());

            properties.clear();
            when(tConnection.isAutoCommit()).thenReturn(true);
            when(ast.getKind()).thenReturn(SqlKind.SELECT);
            ec = new ExecutionContext();
            ec.getParamManager().getProps().put(ConnectionProperties.FORBID_AUTO_COMMIT_TRX, "true");
            when(tConnection.updateTransactionAndConcurrentPolicyForDml(any(), any())).thenCallRealMethod();

            updated = tConnection.updateTransactionAndConcurrentPolicyForDml(plan, ec);
            Assert.assertTrue(updated);
            Assert.assertEquals(trx, ec.getTransaction());
        }

    }

    /**
     * Tests whether the method correctly sets the MASTER property when given a valid 'groupindex:0' hint.
     */
    @Test
    public void testTransformGroupIndexHintToMasterSlaveForMaster() {
        Map<String, Object> extraCmd = new HashMap<>();
        String groupHint = "groupindex:0";

        TConnection.transformGroupIndexHintToMasterSlave(groupHint, extraCmd);

        assertEquals(Boolean.TRUE, extraCmd.get(ConnectionProperties.MASTER));
    }

    /**
     * Tests whether the method correctly sets the SLAVE property when given a valid 'groupindex:1' hint.
     */
    @Test
    public void testTransformGroupIndexHintToMasterSlaveForSlave() {
        Map<String, Object> extraCmd = new HashMap<>();
        String groupHint = "groupindex:1";

        TConnection.transformGroupIndexHintToMasterSlave(groupHint, extraCmd);

        assertEquals(Boolean.TRUE, extraCmd.get(ConnectionProperties.SLAVE));
    }

    /**
     * Tests whether the method leaves the extraCmd unchanged when given an empty string as the groupHint.
     */
    @Test
    public void testTransformGroupIndexHintToMasterSlaveWithEmptyString() {
        Map<String, Object> extraCmd = new HashMap<>();
        String groupHint = "";

        TConnection.transformGroupIndexHintToMasterSlave(groupHint, extraCmd);

        assertEquals(0, extraCmd.size());
    }

    /**
     * Tests whether the method leaves the extraCmd unchanged when given null as the groupHint.
     */
    @Test
    public void testTransformGroupIndexHintToMasterSlaveWithNull() {
        Map<String, Object> extraCmd = new HashMap<>();
        String groupHint = null;

        TConnection.transformGroupIndexHintToMasterSlave(groupHint, extraCmd);

        assertEquals(0, extraCmd.size());
    }

    /**
     * Tests whether the method leaves the extraCmd unchanged when given an invalid input as the groupHint.
     */
    @Test
    public void testTransformGroupIndexHintToMasterSlaveWithInvalidInput() {
        Map<String, Object> extraCmd = new HashMap<>();
        String groupHint = "invalid_input";

        TConnection.transformGroupIndexHintToMasterSlave(groupHint, extraCmd);

        assertEquals(0, extraCmd.size());
    }

    @Test
    public void testCheckGroupForSwitchover() throws SQLException {
        final XDataSource ds = mock(XDataSource.class);
        final Set<SwitchoverPerfCollection> collections = new HashSet<>();
        final TConnection tConnection = new TConnection(new TDataSource());
        final Map<String, Map<String, Boolean>> groups = new HashMap<>();
        groups.put("schema", new HashMap<>());
        groups.get("schema").put("group", true);
        try (final MockedStatic<ExecutorContext> staticExecutorContext = mockStatic(ExecutorContext.class);
            final MockedStatic<OptimizerUtils> staticOptimizerUtils = mockStatic(OptimizerUtils.class)) {
            staticOptimizerUtils.when(() -> OptimizerUtils.useExplicitTransaction(any())).thenReturn(true);
            final ExecutorContext executorContext = Mockito.mock(ExecutorContext.class);
            staticExecutorContext.when(() -> ExecutorContext.getContext(Mockito.anyString()))
                .thenReturn(executorContext);
            final TopologyHandler topologyHandler = Mockito.mock(TopologyHandler.class);
            Mockito.when(executorContext.getTopologyHandler()).thenReturn(topologyHandler);
            final TddlGroupExecutor groupExecutor = Mockito.mock(TddlGroupExecutor.class);
            Mockito.when(topologyHandler.get(any())).thenReturn(groupExecutor);
            final TGroupDataSource dataSource = Mockito.mock(TGroupDataSource.class);
            Mockito.when(groupExecutor.getDataSource()).thenReturn(dataSource);
            final OptimizedGroupConfigManager configManager = Mockito.mock(OptimizedGroupConfigManager.class);
            Mockito.when(dataSource.getConfigManager()).thenReturn(configManager);
            final GroupDataSourceHolder groupDataSourceHolder = Mockito.mock(GroupDataSourceHolder.class);
            Mockito.when(configManager.getGroupDataSourceHolder()).thenReturn(groupDataSourceHolder);
            Mockito.when(groupDataSourceHolder.isChangingLeader(Mockito.any())).thenReturn(Pair.of(true, ds));

            final TsoTransaction tsoTransaction = Mockito.mock(TsoTransaction.class);
            tConnection.getExecutionContext().setTransaction(tsoTransaction);
            final TransactionConnectionHolder connectionHolder = Mockito.mock(TransactionConnectionHolder.class);
            Mockito.when(tsoTransaction.getConnectionHolder()).thenReturn(connectionHolder);
            final TGroupDirectConnection connection = Mockito.mock(TGroupDirectConnection.class);
            Mockito.when(connection.isWrapperFor(any())).thenThrow(new RuntimeException("mock throw"));
            Mockito.when(connectionHolder.getAllConnection()).thenReturn(ImmutableSet.of(connection));

            Assert.assertFalse(tConnection.mayBlockByChangingLeader(groups, collections));

            final AutoCommitTransaction autoCommitTransaction = Mockito.mock(AutoCommitTransaction.class);
            tConnection.getExecutionContext().setTransaction(autoCommitTransaction);
            final AutoCommitConnectionHolder autoCommitConnectionHolder =
                Mockito.mock(AutoCommitConnectionHolder.class);
            Mockito.when(autoCommitTransaction.getConnectionHolder()).thenReturn(autoCommitConnectionHolder);
            Mockito.when(autoCommitConnectionHolder.getAllConnection()).thenReturn(ImmutableSet.of(connection));

            Assert.assertFalse(tConnection.mayBlockByChangingLeader(groups, collections));
        }
        tConnection.mayBlockByChangingLeader(groups, collections);
    }

    @Test
    public void testSwitchover() throws InterruptedException {
        final TConnection tConnection = new TConnection(new TDataSource());
        final ExecutionContext executionContext = Mockito.mock(ExecutionContext.class);
        tConnection.rescheduleIfSwitchover(null, executionContext);

        XConnectionManager.getInstance().markAnyOneChangingLeader();
        final TConnection spyConn = Mockito.spy(tConnection);
        doReturn(true).when(spyConn).mayBlockByChangingLeader(any(), any());
        final RelNode rel = Mockito.mock(RelNode.class);
        final ExecutionPlan executionPlan = Mockito.mock(ExecutionPlan.class);
        Mockito.when(executionPlan.getPlan()).thenReturn(rel);
        try {
            spyConn.rescheduleIfSwitchover(executionPlan, executionContext);
            Assert.fail();
        } catch (CclRescheduleException e) {
            final ServerConnection serverConnection = Mockito.mock(ServerConnection.class);
            e.getRescheduleCallback().apply(serverConnection);
        }

        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.SWITCHOVER_WAIT_TIMEOUT_IN_MILLIS, "1");
        Thread.sleep(2);
        spyConn.rescheduleIfSwitchover(executionPlan, executionContext);
    }
}
