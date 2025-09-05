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

package com.alibaba.polardbx.optimizer.core.planner.Xplanner;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.BaseTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.LogicalDynamicValues;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.PhyQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSelect;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mockStatic;

public class PartitionGatherTest {
    @Test
    public void testRel() {
        final RelNode rel = Mockito.mock(RelNode.class);
        final PartitionGather gather = new PartitionGather(null);
        gather.go(rel);
        Assert.assertTrue(gather.getTargetGroups().isEmpty());
    }

    @Test
    public void testLogicalViewNullTable() {
        final LogicalView rel = Mockito.mock(LogicalView.class);
        Mockito.when(rel.getSchemaName()).thenReturn("schema");
        Mockito.when(rel.getTargetTables(any())).thenReturn(null);

        final PartitionGather gather = new PartitionGather(null);
        gather.go(rel);
        Assert.assertTrue(gather.getTargetGroups().isEmpty());
    }

    @Test
    public void testLogicalView() {
        final LogicalView rel = Mockito.mock(LogicalView.class);
        Mockito.when(rel.getSchemaName()).thenReturn("schema");
        final Map<String, List<List<String>>> tables = new HashMap<>();
        tables.put("group0", ImmutableList.of(ImmutableList.of("tb0")));
        tables.put("group1", ImmutableList.of(ImmutableList.of("tb1")));
        Mockito.when(rel.getTargetTables(any())).thenReturn(tables);
        Mockito.when(rel.getLockMode()).thenReturn(SqlSelect.LockMode.UNDEF);

        final PartitionGather gather = new PartitionGather(null);
        gather.go(rel);
        Assert.assertEquals(1, gather.getTargetGroups().size());
        Assert.assertEquals(2, gather.getTargetGroups().get("schema").size());
        Assert.assertFalse(gather.getTargetGroups().get("schema").get("group0"));
        Assert.assertFalse(gather.getTargetGroups().get("schema").get("group1"));
    }

    @Test
    public void testLogicalViewSharedLock() {
        final LogicalView rel = Mockito.mock(LogicalView.class);
        Mockito.when(rel.getSchemaName()).thenReturn("schema");
        final Map<String, List<List<String>>> tables = new HashMap<>();
        tables.put("group0", ImmutableList.of(ImmutableList.of("tb0")));
        tables.put("group1", ImmutableList.of(ImmutableList.of("tb1")));
        Mockito.when(rel.getTargetTables(any())).thenReturn(tables);
        Mockito.when(rel.getLockMode()).thenReturn(SqlSelect.LockMode.SHARED_LOCK);

        final PartitionGather gather = new PartitionGather(null);
        gather.go(rel);
        Assert.assertEquals(1, gather.getTargetGroups().size());
        Assert.assertEquals(2, gather.getTargetGroups().get("schema").size());
        Assert.assertTrue(gather.getTargetGroups().get("schema").get("group0"));
        Assert.assertTrue(gather.getTargetGroups().get("schema").get("group1"));
    }

    @Test
    public void testLogicalViewExclusiveLock() {
        final LogicalView rel = Mockito.mock(LogicalView.class);
        Mockito.when(rel.getSchemaName()).thenReturn("schema");
        final Map<String, List<List<String>>> tables = new HashMap<>();
        tables.put("group0", ImmutableList.of(ImmutableList.of("tb0")));
        tables.put("group1", ImmutableList.of(ImmutableList.of("tb1")));
        Mockito.when(rel.getTargetTables(any())).thenReturn(tables);
        Mockito.when(rel.getLockMode()).thenReturn(SqlSelect.LockMode.EXCLUSIVE_LOCK);

        final PartitionGather gather = new PartitionGather(null);
        gather.go(rel);
        Assert.assertEquals(1, gather.getTargetGroups().size());
        Assert.assertEquals(2, gather.getTargetGroups().get("schema").size());
        Assert.assertTrue(gather.getTargetGroups().get("schema").get("group0"));
        Assert.assertTrue(gather.getTargetGroups().get("schema").get("group1"));
    }

    @Test
    public void testLogicalInsert0() {
        final ExecutionContext ec = Mockito.mock(ExecutionContext.class);
        final Map<Integer, ParameterContext> params = new HashMap<>();
        Mockito.when(ec.getParamMap()).thenReturn(params);
        Mockito.when(ec.getSchemaName()).thenReturn("schema");
        final LogicalInsert rel = Mockito.mock(LogicalInsert.class);
        final LogicalDynamicValues input = Mockito.mock(LogicalDynamicValues.class);
        Mockito.when(rel.getInput()).thenReturn(input);
        Mockito.when(rel.isSourceSelect()).thenReturn(false);
        Mockito.when(rel.getBatchSize()).thenReturn(0);
        Mockito.when(rel.getTargetTablesHintCache()).thenReturn(null);

        try (MockedStatic<RexUtils> mockRexUtils = mockStatic(RexUtils.class)) {
            mockRexUtils.when(() -> RexUtils.calculateAndUpdateAllRexCallParams(any(), any())).then(i -> null);
            mockRexUtils.when(() -> RexUtils.updateParam(any(), any(), anyBoolean(), any())).then(i -> null);

            final List<RelNode> inputs = new ArrayList<>();
            inputs.add(Mockito.mock(RelNode.class));
            final PhyTableOperation phy0 = Mockito.mock(PhyTableOperation.class);
            final PhyTableOperation phy1 = Mockito.mock(PhyTableOperation.class);
            inputs.add(phy0);
            inputs.add(phy1);
            Mockito.when(rel.getPhyPlanForDisplay(any(), any())).thenReturn(inputs);
            Mockito.when(phy0.getSchemaName()).thenReturn("schema");
            Mockito.when(phy0.getDbIndex()).thenReturn("group0");
            Mockito.when(phy1.getSchemaName()).thenReturn("schema");
            Mockito.when(phy1.getDbIndex()).thenReturn("group1");

            final PartitionGather gather = new PartitionGather(ec);
            gather.go(rel);
        }
    }

    @Test
    public void testLogicalInsert1() {
        final ExecutionContext ec = Mockito.mock(ExecutionContext.class);
        Mockito.when(ec.getParamMap()).thenReturn(null);
        Mockito.when(ec.getSchemaName()).thenReturn(null);
        final LogicalInsert rel = Mockito.mock(LogicalInsert.class);
        final LogicalDynamicValues input = Mockito.mock(LogicalDynamicValues.class);
        Mockito.when(rel.getInput()).thenReturn(input);
        Mockito.when(rel.isSourceSelect()).thenReturn(true);
        Mockito.when(rel.getBatchSize()).thenReturn(1);
        Mockito.when(rel.getTargetTablesHintCache()).thenReturn(new HashMap<>());

        try (MockedStatic<RexUtils> mockRexUtils = mockStatic(RexUtils.class)) {
            mockRexUtils.when(() -> RexUtils.calculateAndUpdateAllRexCallParams(any(), any())).then(i -> null);
            mockRexUtils.when(() -> RexUtils.updateParam(any(), any(), anyBoolean(), any())).then(i -> null);

            final List<RelNode> inputs = new ArrayList<>();
            inputs.add(Mockito.mock(RelNode.class));
            final PhyTableOperation phy0 = Mockito.mock(PhyTableOperation.class);
            final PhyTableOperation phy1 = Mockito.mock(PhyTableOperation.class);
            inputs.add(phy0);
            inputs.add(phy1);
            Mockito.when(rel.getPhyPlanForDisplay(any(), any())).thenReturn(inputs);
            Mockito.when(phy0.getSchemaName()).thenReturn("schema");
            Mockito.when(phy0.getDbIndex()).thenReturn("group0");
            Mockito.when(phy1.getSchemaName()).thenReturn("schema");
            Mockito.when(phy1.getDbIndex()).thenThrow(new RuntimeException("mock throw"));

            final PartitionGather gather = new PartitionGather(ec);
            gather.go(rel);
        }
    }

    @Test
    public void testBaseTableOperation() {
        final ExecutionContext ec = Mockito.mock(ExecutionContext.class);
        Mockito.when(ec.getParamMap()).thenReturn(null);
        final BaseTableOperation rel = Mockito.mock(BaseTableOperation.class);
        Mockito.when(rel.getSchemaName()).thenReturn("schema");
        Mockito.when(rel.getKind()).thenReturn(SqlKind.SELECT);
        Pair<String, Map<Integer, ParameterContext>> dbIndexAndParam = new Pair<>("group0", null);
        Mockito.when(rel.getDbIndexAndParam(any(), any(), any())).thenReturn(dbIndexAndParam);

        Mockito.when(rel.withLock()).thenReturn(true);
        final PartitionGather gather = new PartitionGather(ec);
        gather.go(rel);
        Assert.assertEquals(1, gather.getTargetGroups().size());
        Assert.assertEquals(1, gather.getTargetGroups().get("schema").size());
        Assert.assertTrue(gather.getTargetGroups().get("schema").get("group0"));

        dbIndexAndParam = new Pair<>("group1", null);
        Mockito.when(rel.getDbIndexAndParam(any(), any(), any())).thenReturn(dbIndexAndParam);
        Mockito.when(rel.withLock()).thenReturn(false);
        gather.go(rel);
        Assert.assertEquals(1, gather.getTargetGroups().size());
        Assert.assertEquals(2, gather.getTargetGroups().get("schema").size());
        Assert.assertTrue(gather.getTargetGroups().get("schema").get("group0"));
        Assert.assertFalse(gather.getTargetGroups().get("schema").get("group1"));

        Mockito.when(rel.withLock()).thenReturn(true);
        gather.go(rel);
        Assert.assertEquals(1, gather.getTargetGroups().size());
        Assert.assertEquals(2, gather.getTargetGroups().get("schema").size());
        Assert.assertTrue(gather.getTargetGroups().get("schema").get("group0"));
        Assert.assertTrue(gather.getTargetGroups().get("schema").get("group1"));
    }

    @Test
    public void testPhyQueryOperation() {
        final PhyQueryOperation rel = Mockito.mock(PhyQueryOperation.class);
        Mockito.when(rel.getSchemaName()).thenReturn("schema");
        Mockito.when(rel.getKind()).thenReturn(SqlKind.SELECT);

        Mockito.when(rel.getDbIndex()).thenReturn("group0");
        Mockito.when(rel.getLockMode()).thenReturn(SqlSelect.LockMode.SHARED_LOCK);
        final PartitionGather gather = new PartitionGather(null);
        gather.go(rel);
        Assert.assertEquals(1, gather.getTargetGroups().size());
        Assert.assertEquals(1, gather.getTargetGroups().get("schema").size());
        Assert.assertTrue(gather.getTargetGroups().get("schema").get("group0"));

        Mockito.when(rel.getDbIndex()).thenReturn("group1");
        Mockito.when(rel.getLockMode()).thenReturn(SqlSelect.LockMode.UNDEF);
        gather.go(rel);
        Assert.assertEquals(1, gather.getTargetGroups().size());
        Assert.assertEquals(2, gather.getTargetGroups().get("schema").size());
        Assert.assertTrue(gather.getTargetGroups().get("schema").get("group0"));
        Assert.assertFalse(gather.getTargetGroups().get("schema").get("group1"));

        Mockito.when(rel.getLockMode()).thenReturn(SqlSelect.LockMode.EXCLUSIVE_LOCK);
        gather.go(rel);
        Assert.assertEquals(1, gather.getTargetGroups().size());
        Assert.assertEquals(2, gather.getTargetGroups().get("schema").size());
        Assert.assertTrue(gather.getTargetGroups().get("schema").get("group0"));
        Assert.assertTrue(gather.getTargetGroups().get("schema").get("group1"));
    }
}
