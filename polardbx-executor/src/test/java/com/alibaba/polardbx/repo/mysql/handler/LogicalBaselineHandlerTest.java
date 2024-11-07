package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.extension.ExtensionLoader;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.utils.ExplainExecutorUtil;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.planmanager.BaselineInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;
import com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * @author fangwu
 */
public class LogicalBaselineHandlerTest {
    @After
    public void cleanup() {
        MetaDbInstConfigManager.setConfigFromMetaDb(true);
    }

    @BeforeClass
    public static void setUp() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
    }

    @Test
    public void testBaselineDeletePlan() {
        String sql = "select * from t where c1 = 1";
        try (
            MockedStatic<ExtensionLoader> extensionLoaderMockedStatic = mockStatic(ExtensionLoader.class);
            MockedStatic<SyncManagerHelper> syncManagerHelperMockedStatic = mockStatic(SyncManagerHelper.class)) {
            extensionLoaderMockedStatic.when(() -> ExtensionLoader.load(Mockito.any())).thenReturn(null);
            syncManagerHelperMockedStatic.when(() -> SyncManagerHelper.sync(Mockito.any(), Mockito.any()))
                .thenReturn(null);

            LogicalBaselineHandler baselineHandler = new LogicalBaselineHandler(null);
            PlanManager planManager = Mockito.mock(PlanManager.class);
            Map<String, BaselineInfo> baselineInfoMap = Maps.newHashMap();
            Map<String, Map<String, BaselineInfo>> schemaBaselineInfoMap = Maps.newHashMap();
            schemaBaselineInfoMap.put("test_schema", baselineInfoMap);
            int targetId = 123;
            BaselineInfo baselineInfo = new BaselineInfo(sql, Sets.newHashSet(Pair.of("t", "c1")));
            PlanInfo planInfo = mock(PlanInfo.class);
            when(planInfo.getId()).thenReturn(targetId);
            baselineInfo.addAcceptedPlan(planInfo);
            baselineInfoMap.put(sql, baselineInfo);
            Mockito.when(planManager.getBaselineMap()).thenReturn(schemaBaselineInfoMap);
            List<Long> idList = Lists.newArrayList();
            ExecutionContext ec = new ExecutionContext();
            try {
                baselineHandler.baselineLPCVD(idList, ec, "DELETE", planManager);
            } catch (Exception e) {
                e.printStackTrace();
                assert e.getMessage().equals(
                    "ERR-CODE: [TDDL-7001][ERR_BASELINE] Baseline error: not support baseline DELETE statement without baselineId ");
            }
            idList.add(11111111L);
            idList.add(11111211L);
            idList.add(-11111111L);
            idList.add(Long.valueOf(targetId));
            Cursor c = baselineHandler.baselineLPCVD(idList, ec, "DELETE_PLAN", planManager);

            Row r = null;
            r = c.next();
            Assert.assertTrue(r.getString(0).equals("11111111"));
            Assert.assertTrue(r.getString(1).equals("not found"));

            r = c.next();
            Assert.assertTrue(r.getString(0).equals("11111211"));
            Assert.assertTrue(r.getString(1).equals("not found"));

            r = c.next();
            Assert.assertTrue(r.getString(0).equals("-11111111"));
            Assert.assertTrue(r.getString(1).equals("not found"));

            r = c.next();
            Assert.assertTrue(r.getString(0).equals("123"));
            Assert.assertTrue(r.getString(1).equals("OK"));
        }
    }

    @Test
    public void testBaselineDeleteBaseline() {
        String sql = "select * from t where c1 = 1";
        try (
            MockedStatic<ExtensionLoader> extensionLoaderMockedStatic = mockStatic(ExtensionLoader.class);
            MockedStatic<SyncManagerHelper> syncManagerHelperMockedStatic = mockStatic(SyncManagerHelper.class)) {
            extensionLoaderMockedStatic.when(() -> ExtensionLoader.load(Mockito.any())).thenReturn(null);
            syncManagerHelperMockedStatic.when(() -> SyncManagerHelper.sync(Mockito.any(), Mockito.any()))
                .thenReturn(null);

            LogicalBaselineHandler baselineHandler = new LogicalBaselineHandler(null);
            PlanManager planManager = Mockito.mock(PlanManager.class);
            Map<String, BaselineInfo> baselineInfoMap = Maps.newHashMap();
            Map<String, Map<String, BaselineInfo>> schemaBaselineInfoMap = Maps.newHashMap();
            schemaBaselineInfoMap.put("test_schema", baselineInfoMap);
            long targetId = sql.hashCode();
            BaselineInfo baselineInfo = new BaselineInfo(sql, Sets.newHashSet(Pair.of("t", "c1")));
            baselineInfoMap.put(sql, baselineInfo);
            Mockito.when(planManager.getBaselineMap()).thenReturn(schemaBaselineInfoMap);
            List<Long> idList = Lists.newArrayList();
            ExecutionContext ec = new ExecutionContext();
            try {
                baselineHandler.baselineLPCVD(idList, ec, "DELETE", planManager);
            } catch (Exception e) {
                e.printStackTrace();
                assert e.getMessage().equals(
                    "ERR-CODE: [TDDL-7001][ERR_BASELINE] Baseline error: not support baseline DELETE statement without baselineId ");
            }
            idList.add(11111111L);
            idList.add(11111211L);
            idList.add(-11111111L);
            idList.add(targetId);
            Cursor c = baselineHandler.baselineLPCVD(idList, ec, "DELETE", planManager);

            Row r = null;
            r = c.next();
            Assert.assertTrue(r.getString(0).equals("11111111"));
            Assert.assertTrue(r.getString(1).equals("not found"));

            r = c.next();
            Assert.assertTrue(r.getString(0).equals("11111211"));
            Assert.assertTrue(r.getString(1).equals("not found"));

            r = c.next();
            Assert.assertTrue(r.getString(0).equals("-11111111"));
            Assert.assertTrue(r.getString(1).equals("not found"));

            r = c.next();
            Assert.assertTrue(r.getString(0).equals("1122414013"));
            Assert.assertTrue(r.getString(1).equals("OK"));
        }
    }

    @Test
    public void testBaselineAdd() {
        String sql = "select * from t where c1 = 1";
        LogicalBaselineHandler baselineHandler = new LogicalBaselineHandler(null);
        ExecutionContext ec = new ExecutionContext("test_schema");
        try {
            baselineHandler.baselineAdd(null, sql, ec, false, false, null, null);
            Assert.fail();
        } catch (Exception e) {
            e.printStackTrace();
            assert e.getMessage().equals(
                "ERR-CODE: [TDDL-7001][ERR_BASELINE] Baseline error: not support baseline add statement without hint ");
        }

        String hint = "/*TDDL:BASELINE*/";
        Planner planner = Mockito.mock(Planner.class);
        ExecutionPlan executionPlan = new ExecutionPlan(null, null, null);
        executionPlan.setConstantParams(Maps.newHashMap());
        Mockito.when(planner.plan(Mockito.anyString(), Mockito.eq(ec))).thenReturn(executionPlan);
        try {
            baselineHandler.baselineAdd(hint, sql, ec, false, false, planner, null);
            Assert.fail();
        } catch (Exception e) {
            e.printStackTrace();
            assert e.getMessage().equals(
                "ERR-CODE: [TDDL-7001][ERR_BASELINE] Baseline error: not support baseline add plan with generated column substitution ");
        }
        PlanManager planManager = Mockito.mock(PlanManager.class);
        Map<String, BaselineInfo> baselineInfoMap = Maps.newHashMap();
        BaselineInfo baselineInfo = new BaselineInfo(sql, Sets.newHashSet(Pair.of("t", "c1")));
        baselineInfoMap.put(sql, baselineInfo);
        Mockito.when(planManager.getBaselineMap(Mockito.anyString())).thenReturn(baselineInfoMap);
        executionPlan.setConstantParams(null);

        Cursor cursor = baselineHandler.baselineAdd(hint, sql, ec, false, true, planner, planManager);
        Row r = cursor.next();
        String info = r.getString(2);
        Assert.assertTrue(info.equals("ExecutionPlan exists"));
        cursor.close(null);

        Mockito.when(planManager.getBaselineMap(Mockito.anyString())).thenReturn(Maps.newHashMap());
        Mockito.when(planManager.createBaselineInfo(Mockito.anyString(), Mockito.any(), Mockito.any()))
            .thenReturn(baselineInfo);
        try (
            MockedStatic<ExtensionLoader> extensionLoaderMockedStatic = mockStatic(ExtensionLoader.class);
            MockedStatic<SyncManagerHelper> syncManagerHelperMockedStatic = mockStatic(SyncManagerHelper.class);
            MockedStatic<PlanManagerUtil> planManagerUtil = mockStatic(PlanManagerUtil.class);
            MockedStatic<ExplainExecutorUtil> explainExecutorUtil = mockStatic(ExplainExecutorUtil.class);) {
            extensionLoaderMockedStatic.when(() -> ExtensionLoader.load(Mockito.any())).thenReturn(null);
            syncManagerHelperMockedStatic.when(() -> SyncManagerHelper.sync(Mockito.any(), Mockito.any()))
                .thenReturn(null);
            cursor = baselineHandler.baselineAdd(hint, sql, ec, false, true, planner, planManager);
            r = cursor.next();
            info = r.getString(2);
            Assert.assertTrue(info.startsWith("HINT BIND :"));
            cursor.close(null);

            PlanInfo planInfo = new PlanInfo("", 1, 0.0D, "", "", 0);
            planInfo.setFixed(true);
            ArrayResultCursor result = new ArrayResultCursor("baseline");
            result.addColumn("PLAN", DataTypes.StringType);
            result.addRow(new Object[] {"test plan"});
            planManagerUtil.when(() -> PlanManagerUtil.baselineSupported(Mockito.any())).thenReturn(true);
            explainExecutorUtil.when(() -> ExplainExecutorUtil.explain(Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(result);
            PlanInfo planInfo1 = new PlanInfo("", 1, 0.0D, "", "", 0);
            planInfo1.setFixed(true);
            Mockito.when(planManager.createPlanInfo(Mockito.anyString(), Mockito.any(), Mockito.any(), Mockito.anyInt(),
                Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(planInfo1);
            Mockito.when(planManager.getBaselineMap(Mockito.anyString())).thenReturn(baselineInfoMap);
            baselineInfo.addAcceptedPlan(planInfo);
            planInfo.setFixed(true);

            cursor = baselineHandler.baselineAdd(hint, sql, ec, true, false, planner, planManager);
            r = cursor.next();
            info = r.getString(2);
            Assert.assertTrue(info.equalsIgnoreCase("fixed plan exist"));
            cursor.close(null);
            cursor = baselineHandler.baselineAdd(hint, sql, ec, false, false, planner, planManager);
            r = cursor.next();
            info = r.getString(2);
            Assert.assertTrue(info.equalsIgnoreCase("fixed plan exist"));
            cursor.close(null);

            planInfo.setFixed(false);

            cursor = baselineHandler.baselineAdd(hint, sql, ec, false, false, planner, planManager);
            r = cursor.next();
            info = r.getString(2);
            Assert.assertTrue(info.equalsIgnoreCase("ExecutionPlan exists"));
            cursor.close(null);

            baselineInfo.getAcceptedPlans().clear();

            cursor = baselineHandler.baselineAdd(hint, sql, ec, false, false, planner, planManager);
            r = cursor.next();
            info = r.getString(2);
            Assert.assertTrue(info.equalsIgnoreCase("OK"));
            cursor.close(null);
        }
    }
}
