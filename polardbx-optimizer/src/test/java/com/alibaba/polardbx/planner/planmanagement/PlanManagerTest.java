package com.alibaba.polardbx.planner.planmanagement;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.metadb.table.BaselineInfoAccessor;
import com.alibaba.polardbx.gms.module.LogLevel;
import com.alibaba.polardbx.gms.module.LogPattern;
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.gms.node.LeaderStatusBridge;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.gms.util.SyncUtil;
import com.alibaba.polardbx.optimizer.planmanager.BaselineInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
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
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.alibaba.polardbx.gms.module.LogPattern.UNEXPECTED;
import static javax.naming.ldap.Control.CRITICAL;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
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
        planManager.invalidateTable(schema1, "X1");
        planManager.invalidateTable(schema2, "wrong_table");
        planManager.invalidateTable(schema3, "x3");

        Assert.assertTrue(!planManager.resources().contains(schema1 + " baseline size:"));
        Assert.assertTrue(planManager.resources().contains(schema2 + " baseline size:" + 1));
        Assert.assertTrue(planManager.resources().contains(schema3 + " baseline size:" + 1));

        planManager.invalidateTable(schema3, "X3", true);
        Assert.assertTrue(!planManager.resources().contains(schema3 + " baseline size:" + 1));
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
