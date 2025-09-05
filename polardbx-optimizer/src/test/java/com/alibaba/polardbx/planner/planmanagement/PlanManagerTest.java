package com.alibaba.polardbx.planner.planmanagement;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.module.LogLevel;
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.gms.node.LeaderStatusBridge;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.gms.util.SyncUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.rel.GatherReferencedGsiNameRelVisitor;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.alibaba.polardbx.optimizer.planmanager.BaselineInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;
import com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil;
import com.alibaba.polardbx.optimizer.view.SystemTableView;
import com.alibaba.polardbx.optimizer.view.ViewManager;
import com.google.common.collect.Sets;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexTableInputRef;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import java.sql.Connection;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.alibaba.polardbx.gms.module.LogPattern.UNEXPECTED;
import static com.alibaba.polardbx.optimizer.planmanager.PlanManager.PLAN_SOURCE.SPM_FIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author fangwu
 */
public class PlanManagerTest {
    static String schema1 = "plan_manager_test_schema1";
    static String schema2 = "plan_manager_test_schema2";
    static String schema3 = "plan_manager_test_schema3";

    PlanManager planManager;
    MockedStatic<LeaderStatusBridge> bridgeStatic;

    @Mock
    private ModuleLogInfo mockModuleLogInfo;

    private Map<String, Map<String, BaselineInfo>> baselineMap;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        baselineMap = new HashMap<>();
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        planManager = spy(mock(PlanManager.class));
        bridgeStatic = mockStatic(LeaderStatusBridge.class);
    }

    @After
    public void cleanUp() {
        if (bridgeStatic != null) {
            bridgeStatic.close();
        }
    }

    @Test
    public void testInvalidateSchema() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        PlanManager planManager = PlanManager.getInstance();
        buildSchema(schema1, "xx", false, planManager);
        buildSchema(schema2, "x1", true, planManager);

        System.out.println(planManager.resources());
        Assert.assertTrue(planManager.resources().contains(schema1 + " baseline size:"));
        Assert.assertTrue(planManager.resources().contains(schema2 + " baseline size:"));
        planManager.invalidateSchema(schema1);
        System.out.println(planManager.resources());
        Assert.assertTrue(!planManager.resources().contains(schema1 + " baseline size:"));
        Assert.assertTrue(planManager.resources().contains(schema2 + " baseline size:"));
        planManager.invalidateSchema(schema2);
        System.out.println(planManager.resources());
        Assert.assertTrue(!planManager.resources().contains(schema2 + " baseline size:"));
    }

    @Test
    public void testInvalidateTable() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        PlanManager planManager = PlanManager.getInstance();
        buildSchema(schema1, "x1", false, planManager);
        buildSchema(schema2, "x2", false, planManager);
        buildSchema(schema3, "x3", true, planManager);

        Assert.assertTrue(planManager.resources().contains(schema1 + " baseline size:" + 1));
        Assert.assertTrue(planManager.resources().contains(schema2 + " baseline size:" + 1));
        try (MockedStatic<OptimizerContext> optimizerContextMockedStatic = mockStatic(OptimizerContext.class)) {
            optimizerContextMockedStatic.when(() -> OptimizerContext.getContext(anyString()))
                .thenReturn(mock(OptimizerContext.class));
            planManager.invalidateTable(schema1, "X1");
            planManager.invalidateTable(schema2, "wrong_table");
            planManager.invalidateTable(schema3, "x3");

            Assert.assertTrue(!planManager.resources().contains(schema1 + " baseline size:"));
            Assert.assertTrue(planManager.resources().contains(schema2 + " baseline size:" + 1));
            Assert.assertTrue(planManager.resources().contains(schema3 + " baseline size:" + 1));

            planManager.invalidateTable(schema3, "X3", true);
            Assert.assertTrue(!planManager.resources().contains(schema3 + " baseline size:" + 1));
        }

    }

    /**
     * When the baseline map is empty, no exceptions should be thrown and it returns immediately.
     */
    @Test
    public void testDeleteBaselineWhenBaselineMapIsEmpty() {
        // Prepare
        String schema = "test_schema";
        Integer baselineId = 1;
        baselineMap.clear();

        // Execute
        PlanManager.deleteBaseline(schema, baselineId, baselineMap);
    }

    /**
     * Successfully deletes an existing baseline and invokes the database access layer's delete method.
     */
    @Test
    public void testDeleteBaselineSuccessfully() {
        // Prepare
        String schema = "test_schema";
        Integer baselineId = 49;
        Map<String, BaselineInfo> bMap = new HashMap<>();
        BaselineInfo info = new BaselineInfo("1", null);
        bMap.put("key", info);
        baselineMap.put(schema, bMap);

        try (MockedStatic<SyncUtil> syncUtilMockedStatic = mockStatic(SyncUtil.class)) {
            syncUtilMockedStatic.when(SyncUtil::isNodeWithSmallestId).thenReturn(true);

            // Execute
            PlanManager.deleteBaseline(schema, baselineId, baselineMap);

            // Verify
            assertFalse(bMap.containsKey("key"));
        }
    }

    /**
     * Test Case 5: During deletion process encounters an error, logs are recorded properly.
     */
    @Test
    public void testDeleteBaselineWithErrorDuringDeletion() {
        // Prepare
        String schema = "test_schema";
        Map<String, BaselineInfo> bMap = new HashMap<>();
        BaselineInfo info = new BaselineInfo("test sql", null);
        Integer baselineId = info.getId();
        bMap.put("key", info);
        baselineMap.put(schema, bMap);

        try (MockedStatic<SyncUtil> syncUtilMockedStatic = mockStatic(SyncUtil.class);
            MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class);
            MockedStatic<ModuleLogInfo> moduleLogInfoMockedStatic = mockStatic(ModuleLogInfo.class);
        ) {
            syncUtilMockedStatic.when(SyncUtil::isNodeWithSmallestId).thenReturn(true);
            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(mock(Connection.class));
            moduleLogInfoMockedStatic.when(ModuleLogInfo::getInstance).thenReturn(mockModuleLogInfo);

            // Execute
            PlanManager.deleteBaseline(schema, baselineId, baselineMap);

            // Verify
            verify(mockModuleLogInfo).logRecord(eq(Module.SPM),
                eq(UNEXPECTED),
                argThat(arr -> arr.length == 2 && arr[0].equals("BASELINE DELETE")),
                eq(LogLevel.CRITICAL),
                any(RuntimeException.class));
        }
    }

    @Test
    public void testInvalidateTableParrallel() throws InterruptedException {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        PlanManager planManager = PlanManager.getInstance();
        AtomicBoolean testFailed = new AtomicBoolean(false);
        // test processor work, random add or/invalidate table
        Runnable r = () -> {
            try {
                while (true) {
                    Thread.sleep(10);

                    Random random = new Random();
                    if (random.nextBoolean()) {
                        buildSchema(schema1, "x1", false, planManager);
                        buildSchema(schema2, "x2", false, planManager);
                        buildSchema(schema3, "x3", true, planManager);
                    } else {
                        planManager.invalidateTable(schema1, "X1");
                        planManager.invalidateTable(schema2, "wrong_table");
                        planManager.invalidateTable(schema3, "x3");
                        planManager.invalidateTable(schema3, "X3", true);
                    }
                }
            } catch (InterruptedException e) {
                System.out.println("thread out");
            } catch (Exception e) {
                e.printStackTrace();
                testFailed.set(true);
                Assert.fail("parrallel test fail :" + e.getMessage());
            }
        };

        Thread[] threads = new Thread[10];
        for (int i = 0; i < 10; i++) {
            threads[i] = new Thread(r);
            threads[i].start();
        }

        for (int i = 0; i < 100; i++) {
            Thread.sleep(60);
            if (testFailed.get()) {
                for (int j = 0; j < 10; j++) {
                    threads[i].interrupt();
                }
                Assert.fail();
            }
        }

        Arrays.stream(threads).forEach(t -> t.interrupt());
        for (Thread t : threads) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Tests successful removal of a plan from a baseline and potential deletion of the entire baseline.
     */
    @Test
    public void testDeleteBaselinePlanSuccessfulRemovalAndPotentialDeletionOfBaseline() {
        // Arrange
        String schema = "test_schema";
        String sql = "select * from t1";
        BaselineInfo info = new BaselineInfo(sql, null);
        Integer baselineId = info.getId();
        PlanInfo planInfo1 = new PlanInfo("1", baselineId, 0.0D, "", "", 0);
        PlanInfo planInfo2 = new PlanInfo("2", baselineId, 0.0D, "", "", 0);
        int planInfoId = planInfo1.getId();

        info.addAcceptedPlan(planInfo1);
        info.addUnacceptedPlan(planInfo2);

        Map<String, BaselineInfo> innerMap = new HashMap<>();

        innerMap.put(sql, info);
        baselineMap.put(schema, innerMap);

        // Act
        PlanManager.deleteBaselinePlan(schema, baselineId, planInfoId, baselineMap);

        assertTrue(innerMap.isEmpty());
    }

    /**
     * Verifies the method handles gracefully when the baseline map is empty.
     */
    @Test
    public void testDeleteBaselinePlanWhenBaselineMapIsEmptyShouldReturnWithoutAction() {
        String schema = "test_schema";
        Integer baselineId = 1;
        int planInfoId = 100;

        PlanManager.deleteBaselinePlan(schema, baselineId, planInfoId, baselineMap);
    }

    /**
     * Checks the method's response when the specified schema is not present in the baseline map.
     */
    @Test
    public void testDeleteBaselinePlanWhenSchemaIsNotPresentInBaselineMapShouldReturnWithoutAction() {
        // Arrange
        String nonExistentSchema = "nonexistent_schema";
        Integer baselineId = 1;
        int planInfoId = 100;

        Map<String, Map<String, BaselineInfo>> anotherBaselineMap = new HashMap<>();

        // Act & Assert
        PlanManager.deleteBaselinePlan(nonExistentSchema, baselineId, planInfoId, anotherBaselineMap);
    }

    /**
     * Ensures the method behaves correctly if the baseline ID is not found within the specified schema's map.
     */
    @Test
    public void testDeleteBaselinePlanWhenBaselineIdIsNotFoundShouldReturnWithoutAction() {
        // Arrange
        String schema = "test_schema";
        Integer mismatchedBaselineId = 999;
        int planInfoId = 100;

        Map<String, BaselineInfo> innerMap = new HashMap<>();
        BaselineInfo info = mock(BaselineInfo.class);
        when(info.getId()).thenReturn(1); // Different ID

        innerMap.put("key", info);
        baselineMap.put(schema, innerMap);

        // Act & Assert
        PlanManager.deleteBaselinePlan(schema, mismatchedBaselineId, planInfoId, baselineMap);
        verify(info, never()).removeAcceptedPlan(anyInt());
    }

    /**
     * RebuildAtLoadPlan 测试用例1: 正常情况下重建计划并返回结果
     */
    @Test
    public void testHandleRebuildAtLoadPlanNormalCase() {
        // 准备
        Planner planner = mock(Planner.class);
        BaselineInfo baselineInfo = mock(BaselineInfo.class);
        SqlParameterized sqlParameterized = mock(SqlParameterized.class);
        String hint = "hint";
        String sql = "SELECT * FROM table";
        RelNode retPlan = mock(RelNode.class);
        when(retPlan.accept(any(GatherReferencedGsiNameRelVisitor.class))).thenReturn(retPlan);
        when(sqlParameterized.getSql()).thenReturn(sql);
        when(baselineInfo.getHint()).thenReturn(hint);
        when(baselineInfo.computeRebuiltAtLoadPlanIfNotExists(any())).thenCallRealMethod();
        when(baselineInfo.getRebuildAtLoadPlan()).thenCallRealMethod();
        ExecutionPlan p = new ExecutionPlan(null, retPlan, null);
        when(planner.plan(anyString(), any())).thenReturn(p);
        ExecutionContext ec = new ExecutionContext();
        try (MockedStatic<Planner> mockedStaticPlanner = mockStatic(Planner.class);
            MockedStatic<PlanManagerUtil> mockedStaticPlanManagerUtil = mockStatic(PlanManagerUtil.class);
        ) {
            mockedStaticPlanManagerUtil.when(() -> PlanManagerUtil.getPlanOrigin(any())).thenReturn("AP");
            mockedStaticPlanManagerUtil.when(() -> PlanManagerUtil.relNodeToJson(any())).thenReturn("plan json");
            mockedStaticPlanner.when(Planner::getInstance).thenReturn(planner);
            RelOptCluster cluster = mock(RelOptCluster.class);
            when(retPlan.getCluster()).thenReturn(cluster);
            RelMetadataQuery mq = mock(RelMetadataQuery.class);
            when(cluster.getMetadataQuery()).thenReturn(mq);
            when(mq.getCumulativeCost(any())).thenReturn(new RelOptCostImpl(100D));

            PlanManager.Result result =
                planManager.handleRebuildAtLoadPlan(baselineInfo, retPlan, sqlParameterized, ec, 0);

            // 验证
            assertNotNull(result);
            assertEquals(SPM_FIX, result.source);
            assertSame(retPlan, result.plan);
            verify(baselineInfo, times(0)).resetRebuildAtLoadPlanIfMismatched(anyInt());
        }
    }

    /**
     * RebuildAtLoadPlan 测试用例2: 当表版本不匹配时重新计算计划
     */
    @Test
    public void testHandleRebuildAtLoadPlanMismatchedTableVersion() {
        // 准备
        Planner planner = mock(Planner.class);
        BaselineInfo baselineInfo = mock(BaselineInfo.class);
        SqlParameterized sqlParameterized = mock(SqlParameterized.class);
        String hint = "hint";
        String sql = "SELECT * FROM table";
        RelNode retPlan = mock(RelNode.class);
        when(retPlan.accept(any(GatherReferencedGsiNameRelVisitor.class))).thenReturn(retPlan);
        when(sqlParameterized.getSql()).thenReturn(sql);
        when(baselineInfo.getHint()).thenReturn(hint);
        when(baselineInfo.computeRebuiltAtLoadPlanIfNotExists(any())).thenCallRealMethod();
        when(baselineInfo.getRebuildAtLoadPlan()).thenCallRealMethod();
        ExecutionPlan p = new ExecutionPlan(null, retPlan, null);
        when(planner.plan(anyString(), any())).thenReturn(p);
        ExecutionContext ec = new ExecutionContext();
        try (MockedStatic<Planner> mockedStaticPlanner = mockStatic(Planner.class);
            MockedStatic<PlanManagerUtil> mockedStaticPlanManagerUtil = mockStatic(PlanManagerUtil.class);
        ) {
            mockedStaticPlanManagerUtil.when(() -> PlanManagerUtil.getPlanOrigin(any())).thenReturn("AP");
            mockedStaticPlanManagerUtil.when(() -> PlanManagerUtil.relNodeToJson(any())).thenReturn("plan json");
            mockedStaticPlanner.when(Planner::getInstance).thenReturn(planner);
            RelOptCluster cluster = mock(RelOptCluster.class);
            when(retPlan.getCluster()).thenReturn(cluster);
            RelMetadataQuery mq = mock(RelMetadataQuery.class);
            when(cluster.getMetadataQuery()).thenReturn(mq);
            when(mq.getCumulativeCost(any())).thenReturn(new RelOptCostImpl(100D));

            PlanManager.Result result =
                planManager.handleRebuildAtLoadPlan(baselineInfo, retPlan, sqlParameterized, ec, 100);

            // 验证
            assertNotNull(result);
            assertEquals(SPM_FIX, result.source);
            assertSame(retPlan, result.plan);
            verify(baselineInfo, times(1)).resetRebuildAtLoadPlanIfMismatched(anyInt());
        }
    }

    /**
     * 正常情况下的处理，视图定义存在且引用了指定表。
     */
    @Test
    public void testHandleViewNormalCaseViewExistsAndReferenced() {
        // 准备
        String currentSchema = "current_schema";
        String currentTable = "current_table";
        BaselineInfo baselineInfo = mock(BaselineInfo.class);
        when(baselineInfo.getParameterSql()).thenReturn("parameter_sql");
        String comparisonSchema = "comparison_schema";
        String comparisonTable = "comparison_table";

        Map<String, Set<String>> removalCandidates = new HashMap<>();
        SystemTableView.Row viewRow = mock(SystemTableView.Row.class);

        try (MockedStatic<Planner> plannerMockedStatic = mockStatic(Planner.class);
            MockedStatic<RelMetadataQuery> relMetadataQueryMockedStatic = mockStatic(RelMetadataQuery.class)) {
            doReturn("SELECT * FROM current_table").when(viewRow).getViewDefinition();
            RexTableInputRef.RelTableRef relTableRef = mock(RexTableInputRef.RelTableRef.class);
            when(relTableRef.getQualifiedName()).thenReturn(Arrays.asList(currentSchema, currentTable));
            OptimizerContext optimizerContext = mock(OptimizerContext.class);
            OptimizerContext optimizerContext1 = mock(OptimizerContext.class);
            ViewManager viewMock = mock(ViewManager.class);
            when(viewMock.select(comparisonTable)).thenReturn(viewRow);
            when(optimizerContext.getViewManager()).thenReturn(viewMock);
            when(optimizerContext.getSchemaName()).thenReturn(comparisonSchema);
            when(optimizerContext1.getSchemaName()).thenReturn(currentSchema);
            when(optimizerContext1.getLatestSchemaManager()).thenReturn(mock(SchemaManager.class));
            OptimizerContext.loadContext(optimizerContext);
            OptimizerContext.loadContext(optimizerContext1);

            Planner mockPlanner = mock(Planner.class);
            plannerMockedStatic.when(Planner::getInstance).thenReturn(mockPlanner);

            ExecutionPlan mockExecutionPlan = mock(ExecutionPlan.class);
            when(mockPlanner.plan(eq("SELECT * FROM current_table"), any())).thenReturn(mockExecutionPlan);

            Set<Pair<String, String>> tableSet = Sets.newHashSet();
            tableSet.add(Pair.of(currentSchema, currentTable));
            when(mockExecutionPlan.getTableSet()).thenReturn(tableSet);

            // 执行
            PlanManager.handleView(currentSchema, currentTable, baselineInfo, comparisonSchema, comparisonTable,
                removalCandidates);

            // 验证
            assertTrue(removalCandidates.containsKey(currentSchema));
            assertEquals(1, removalCandidates.get(currentSchema).size());
            assertTrue(removalCandidates.get(currentSchema).contains("parameter_sql"));

            removalCandidates.clear();
            tableSet.clear();
            tableSet.add(Pair.of(null, comparisonTable));

            // 执行
            PlanManager.handleView(comparisonSchema, comparisonTable, baselineInfo, comparisonSchema, comparisonTable,
                removalCandidates);

            // 验证
            assertTrue(removalCandidates.containsKey(comparisonSchema));
            assertEquals(1, removalCandidates.get(comparisonSchema).size());
            assertTrue(removalCandidates.get(comparisonSchema).contains("parameter_sql"));
        }

    }

    @Test
    public void testTryUpdatePlan() {
        RelNode oldPlan = mock(RelNode.class);
        RelNode newPlan = mock(RelNode.class);
        SqlParameterized sqlParameterized = mock(SqlParameterized.class);
        RelOptCluster cluster = mock(RelOptCluster.class);
        RelOptSchema relOptSchema = mock(RelOptSchema.class);

        Planner planner = mock(Planner.class);
        ExecutionPlan executionPlan = mock(ExecutionPlan.class);

        when(planner.plan(anyString(), any())).thenReturn(executionPlan);
        when(executionPlan.getPlan()).thenReturn(newPlan);

        PlanManager.PLAN_SOURCE source;
        ExecutionContext ec = new ExecutionContext();
        try (MockedStatic<Planner> plannerMockedStatic = mockStatic(Planner.class);
            MockedStatic<PlanManagerUtil> planManagerUtilMockedStatic = mockStatic(PlanManagerUtil.class);) {
            planManagerUtilMockedStatic.when(() -> PlanManagerUtil.relNodeToJson(any())).thenReturn("");
            plannerMockedStatic.when(Planner::getInstance).thenReturn(planner);

            PlanInfo planInfo = new PlanInfo(oldPlan, 1, 1D, "", "", 1);

            // test SPM_FIX_PLAN_UPDATE_FOR_ROW_TYPE
            RelDataType oldType = mock(RelDataType.class);
            RelDataType newType = mock(RelDataType.class);

            when(oldPlan.getRowType()).thenReturn(oldType);
            when(newPlan.getRowType()).thenReturn(newType);
            // make type string dis match
            when(oldType.getFullTypeString()).thenReturn("oldType");
            when(newType.getFullTypeString()).thenReturn("newType");

            source = PlanManager.tryUpdatePlan(planInfo, sqlParameterized, cluster, relOptSchema, 1, ec);

            assert source == PlanManager.PLAN_SOURCE.SPM_FIX_PLAN_UPDATE_FOR_ROW_TYPE;
            assert planInfo.getPlan(null, null) == newPlan;

            // test SPM_FIX_PLAN_UPDATE_FOR_INVALID
            when(oldType.getFullTypeString()).thenReturn("sameType");
            when(newType.getFullTypeString()).thenReturn("sameType");
            when(oldPlan.isValid(any(), any())).thenReturn(false);

            source = PlanManager.tryUpdatePlan(planInfo, sqlParameterized, cluster, relOptSchema, 1, ec);

            assert source == PlanManager.PLAN_SOURCE.SPM_FIX_PLAN_UPDATE_FOR_INVALID;
            assert planInfo.getPlan(null, null) == newPlan;

            // test SPM_FIX_DDL_HASHCODE_UPDATE
            planInfo.resetPlan(oldPlan);
            when(oldPlan.isValid(any(), any())).thenReturn(true);

            source = PlanManager.tryUpdatePlan(planInfo, sqlParameterized, cluster, relOptSchema, 1, ec);

            assert source == PlanManager.PLAN_SOURCE.SPM_FIX_DDL_HASHCODE_UPDATE;
            assert planInfo.getPlan(null, null) == newPlan;
        }
    }

    private void buildSchema(String schema, String tableName, boolean hasFixPlan,
                             PlanManager planManager) {
        Map<String, BaselineInfo> schemaMap1 = planManager.getBaselineMap(schema);

        String sql = "select * from " + tableName;
        Set<Pair<String, String>> tableSet = Sets.newHashSet();
        tableSet.add(Pair.of(schema, tableName));
        if (hasFixPlan) {
            schemaMap1.put(sql, BaselineInfoTest.buildBaselineInfoWithFixedPlan(sql, tableSet));
        } else {
            schemaMap1.put(sql, BaselineInfoTest.buildBaselineInfoWithoutFixedPlan(sql, tableSet));
        }
    }

}
