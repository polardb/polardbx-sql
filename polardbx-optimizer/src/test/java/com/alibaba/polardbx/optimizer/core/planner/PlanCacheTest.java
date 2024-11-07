/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.optimizer.core.planner;

import com.alibaba.polardbx.common.properties.BooleanConfigParam;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.parse.SqlParameterizeUtils;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.alibaba.polardbx.planner.common.PlanTestCommon;
import com.clearspring.analytics.util.Lists;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalValues;
import org.junit.Test;
import org.junit.runners.Parameterized;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mockStatic;

public class PlanCacheTest extends PlanTestCommon {
//    private static String schemaName = "tddl";

    private static String[] sqls = {
            "select * from t_shard_id1",
            "select * from t_shard_id1 limit 1",
            "select * from t_shard_id1 order by id",
            "select * from t_shard_id1 order by id limit 1",
            "select * from t_shard_id1 where name='a'",
            "select * from t_shard_id1 where name like '%s'",
            "select * from t_shard_id1 where name='a' limit 10",
    };

    private static String[] inSqls = {
            "select * from t_shard_id1",
            "select * from t_shard_id1 limit 1",
            "select * from t_shard_id1 where name in ('a')",
            "select * from t_shard_id1 where name in ('a', 'b')",
            "select * from t_shard_id1 where name in ('a', 'c', 'e')",
            "select * from t_shard_id1 where name in ('a', 'd', 'e', 'd')",
            "select * from t_shard_id1 where name in ('a', 'b') limit 1",
            "select * from t_shard_id1 where name in ('a') limit 1",
            "select * from t_shard_id1 where name in ('a', 'c', 'e') limit 1",
            "select * from t_shard_id1 where name in ('a', 'j', 'f', 'o') limit 1",
    };

    public PlanCacheTest(String caseName, String targetEnvFile) {
        super(caseName, targetEnvFile);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return ImmutableList.of(new Object[]{"PlanCacheTest", "/com/alibaba/polardbx/planner/planmanagement/PlanManagementPlanTest"});
    }

    @Override
    public void testSql() {

    }

    @Test
    public void testExpireTime() {
        // default config test, 12H
        testExpireTimeEqualsConfig(12 * 3600 * 1000);

        // manual set config test
        int manualExpireTime = 110 * 1000;
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.PLAN_CACHE_EXPIRE_TIME, manualExpireTime + "");
        testExpireTimeEqualsConfig(manualExpireTime);
    }

    private void testExpireTimeEqualsConfig(int expectedTime) {
        int expireTime = DynamicConfig.getInstance().planCacheExpireTime();
        PlanCache planCache = new PlanCache(1);
        assert expireTime == planCache.getPlanCacheExpireTime();
        assert expireTime == expectedTime;
    }

    @Test
    public void testUpperBound() throws ExecutionException {
        int maxSize = 3;
        PlanCache planCache = new PlanCache(maxSize);
        for (String sql : sqls) {
            ExecutionContext executionContext = new ExecutionContext(this.appName);
            SqlParameterized sqlParameterized = SqlParameterizeUtils.parameterize(ByteString.from(sql), null, executionContext, false);
            planCache.get(this.appName, sqlParameterized, executionContext, false);
        }
        if (planCache.getCache().size() > maxSize) {
            Assert.fail("plan cache max size over limit");
        }
    }

    @Test
    public void testInSqlBound() throws ExecutionException {
        int maxSize = 3;
        PlanCache planCache = new PlanCache(maxSize);
        for (String sql : inSqls) {
            ExecutionContext executionContext = new ExecutionContext(this.appName);
            SqlParameterized sqlParameterized = SqlParameterizeUtils.parameterize(ByteString.from(sql), null, executionContext, false);
            planCache.get(this.appName, sqlParameterized, executionContext, false);
        }
        if (planCache.getCache().size() > maxSize) {
            Assert.fail("plan cache in sql size over limit:" + planCache.getCache().size());
        }
    }

    @Test
    public void testInSqlBound2() throws ExecutionException {
        int maxSize = 50;
        PlanCache planCache = new PlanCache(maxSize);
        for (String sql : inSqls) {
            ExecutionContext executionContext = new ExecutionContext(this.appName);
            SqlParameterized sqlParameterized = SqlParameterizeUtils.parameterize(ByteString.from(sql), null, executionContext, false);
            planCache.get(this.appName, sqlParameterized, executionContext, false);
        }
        if (planCache.getCache().size() > inSqls.length) {
            Assert.fail("plan cache in sql size over limit:" + planCache.getCache().size());
        }
    }

    @Test
    public void testColumnarCacheSwitch() throws Exception {
        boolean[] values = new boolean[] {true, false};
        for (boolean cacheColumnar : values) {
            for (boolean cacheColumnarPlanCache : values) {
                for (boolean useColumnar : values) {
                    for (boolean useColumnarPlanCache : values) {
                        columnarPlanCacheSwitch(cacheColumnar, cacheColumnarPlanCache, useColumnar,
                            useColumnarPlanCache);
                    }
                }
            }
        }
    }

    void columnarPlanCacheSwitch(boolean cacheColumnar, boolean cacheColumnarPlanCache, boolean useColumnar,
                                 boolean useColumnarPlanCache) throws Exception {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        boolean config = DynamicConfig.getInstance().colPlanCache();
        ExecutionContext executionContext = new ExecutionContext("test");
        LogicalValues va0 = Mockito.mock(LogicalValues.class);
        LogicalValues va1 = Mockito.mock(LogicalValues.class);
        try (MockedStatic<PlannerContext> plannerContextMockedStatic = mockStatic(PlannerContext.class)) {
            DynamicConfig.getInstance()
                .loadValue(null, ConnectionProperties.ENABLE_COLUMNAR_PLAN_CACHE, String.valueOf(useColumnarPlanCache));
            PlanCache planCache = Mockito.mock(PlanCache.class);
            Mockito.doNothing().when(planCache).invalidateByCacheKey(any());

            SqlParameterized sqlParameterized = new SqlParameterized("", Lists.newArrayList());
            Field tablesField = sqlParameterized.getClass().getDeclaredField("tables");
            tablesField.setAccessible(true);
            tablesField.set(sqlParameterized, Sets.newHashSet());

            plannerContextMockedStatic.when(() -> PlannerContext.getPlannerContext(va0)).thenAnswer(
                invocation -> {
                    PlannerContext pc = Mockito.mock(PlannerContext.class);
                    Mockito.when(pc.isUseColumnar()).thenReturn(cacheColumnar);
                    Mockito.when(pc.isUseColumnarPlanCache())
                        .thenReturn(cacheColumnarPlanCache);
                    return pc;
                });

            plannerContextMockedStatic.when(() -> PlannerContext.getPlannerContext(va1)).thenAnswer(
                invocation -> {
                    PlannerContext pc = Mockito.mock(PlannerContext.class);
                    Mockito.when(pc.isUseColumnar()).thenReturn(useColumnar);
                    Mockito.when(pc.isUseColumnarPlanCache())
                        .thenReturn(executionContext.isColumnarPlanCache());
                    return pc;
                });

            Mockito.when(planCache.getCacheLoader(any(), any(), any(), any(), anyBoolean(), any(), any())).thenAnswer(
                (Answer<Callable<ExecutionPlan>>) invocation -> () -> {
                    ExecutionPlan plan = Mockito.mock(ExecutionPlan.class);
                    Mockito.when(plan.getPlan()).thenReturn(va1);
                    return plan;
                }
            );

            AtomicInteger count = new AtomicInteger(0);
            Mockito.when(planCache.getPlanWithLoader(any(), any())).thenAnswer(
                (Answer<ExecutionPlan>) invocation -> {
                    count.getAndIncrement();
                    if (count.get() == 1) {
                        ExecutionPlan plan = Mockito.mock(ExecutionPlan.class);
                        Mockito.when(plan.getPlan()).thenReturn(va0);
                        return plan;
                    }
                    Object[] args = invocation.getArguments();
                    Callable<ExecutionPlan> loader = (Callable<ExecutionPlan>) args[1];
                    return loader.call();
                });

            Mockito.when(planCache.savePlanCachedKey(any(), any(), any(), any())).thenAnswer(
                (Answer<ExecutionPlan>) invocation -> (ExecutionPlan) invocation.getArguments()[1]);

            Mockito.when(planCache.checkColumnarDisable(any(), any(), any(), any(), any())).thenReturn(null);
            doCallRealMethod().when(planCache).checkColumnarPlanCache(any(), any(), any(), any(), any(), any());
            doCallRealMethod().when(planCache).getFromCache(any(), any(), any(), any(), anyBoolean());
            ExecutionPlan plan = planCache.getFromCache("test", sqlParameterized, null, executionContext, false);

            // row plan cached
            if (!cacheColumnar) {
                // plan must be row
                Assert.check(!PlannerContext.getPlannerContext(plan.getPlan()).isUseColumnar());
                Assert.check(count.get() == 1);
                return;
            }

            // cacheColumnar == true
            if (cacheColumnarPlanCache == useColumnarPlanCache) {
                Assert.check(count.get() == 1);
            }

            if (cacheColumnarPlanCache && useColumnar && useColumnarPlanCache) {
                Assert.check(PlannerContext.getPlannerContext(plan.getPlan()).isUseColumnar());
                Assert.check(PlannerContext.getPlannerContext(plan.getPlan()).isUseColumnarPlanCache());
                Assert.check(count.get() == 1);
            }
            if (cacheColumnarPlanCache && useColumnar && !useColumnarPlanCache) {
                Assert.check(plan == null);
                Assert.check(count.get() == 2);
            }
            if (cacheColumnarPlanCache && !useColumnar && useColumnarPlanCache) {
                Assert.check(PlannerContext.getPlannerContext(plan.getPlan()).isUseColumnar());
                Assert.check(PlannerContext.getPlannerContext(plan.getPlan()).isUseColumnarPlanCache());
                Assert.check(count.get() == 1);
            }
            if (cacheColumnarPlanCache && !useColumnar && !useColumnarPlanCache) {
                Assert.check(!PlannerContext.getPlannerContext(plan.getPlan()).isUseColumnar());
                Assert.check(!PlannerContext.getPlannerContext(plan.getPlan()).isUseColumnarPlanCache());
                Assert.check(count.get() == 2);
            }
            if (!cacheColumnarPlanCache && useColumnar && useColumnarPlanCache) {
                Assert.check(PlannerContext.getPlannerContext(plan.getPlan()).isUseColumnar());
                Assert.check(PlannerContext.getPlannerContext(plan.getPlan()).isUseColumnarPlanCache());
                Assert.check(count.get() == 2);
            }
            if (!cacheColumnarPlanCache && useColumnar && !useColumnarPlanCache) {
                Assert.check(plan == null);
                Assert.check(count.get() == 1);
            }
            if (!cacheColumnarPlanCache && !useColumnar && useColumnarPlanCache) {
                Assert.check(!PlannerContext.getPlannerContext(plan.getPlan()).isUseColumnar());
                Assert.check(PlannerContext.getPlannerContext(plan.getPlan()).isUseColumnarPlanCache());
                Assert.check(count.get() == 2);
            }
            if (!cacheColumnarPlanCache && !useColumnar && !useColumnarPlanCache) {
                Assert.check(plan == null);
                Assert.check(count.get() == 1);
            }
        } finally {
            DynamicConfig.getInstance()
                .loadValue(null, ConnectionProperties.ENABLE_COLUMNAR_PLAN_CACHE, String.valueOf(config));
        }
    }

    @Test
    public void testColumnarSwitch() throws Exception {
        boolean[] values = new boolean[] {true, false};
        for (boolean cacheColumnar : values) {
            for (boolean enableColumnar : values) {
                columnarSwitch(cacheColumnar, enableColumnar);
            }
        }
    }

    void columnarSwitch(boolean cacheColumnar, boolean enableColumnar) throws Exception {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        ExecutionContext executionContext = new ExecutionContext("test");
        LogicalValues va0 = Mockito.mock(LogicalValues.class);
        LogicalValues va1 = Mockito.mock(LogicalValues.class);
        try (MockedStatic<PlannerContext> plannerContextMockedStatic = mockStatic(PlannerContext.class)) {
            DynamicConfig.getInstance().existColumnarNodes(true);

            ParamManager pm = Mockito.mock(ParamManager.class);
            executionContext.setParamManager(pm);
            Mockito.when(pm.getBoolean(any())).thenAnswer(
                invocation -> {
                    BooleanConfigParam param = invocation.getArgument(0);
                    if (param == ConnectionParams.ENABLE_COLUMNAR_OPTIMIZER) {
                        return enableColumnar;
                    }
                    if (param == ConnectionParams.ENABLE_COLUMNAR_OPTIMIZER_WITH_COLUMNAR) {
                        return enableColumnar;
                    }
                    return false;
                }
            );
            PlanCache planCache = Mockito.mock(PlanCache.class);
            Mockito.doNothing().when(planCache).invalidateByCacheKey(any());

            SqlParameterized sqlParameterized = new SqlParameterized("", Lists.newArrayList());
            Field tablesField = sqlParameterized.getClass().getDeclaredField("tables");
            tablesField.setAccessible(true);
            tablesField.set(sqlParameterized, Sets.newHashSet());

            plannerContextMockedStatic.when(() -> PlannerContext.getPlannerContext(va0)).thenAnswer(
                invocation -> {
                    PlannerContext pc = Mockito.mock(PlannerContext.class);
                    Mockito.when(pc.isUseColumnar()).thenReturn(cacheColumnar);
                    Mockito.when(pc.isUseColumnarPlanCache()).thenReturn(true);
                    return pc;
                });

            plannerContextMockedStatic.when(() -> PlannerContext.getPlannerContext(va1)).thenAnswer(
                invocation -> {
                    PlannerContext pc = Mockito.mock(PlannerContext.class);
                    Mockito.when(pc.isUseColumnar()).thenReturn(enableColumnar);
                    Mockito.when(pc.isUseColumnarPlanCache()).thenReturn(true);
                    return pc;
                });

            Mockito.when(planCache.getCacheLoader(any(), any(), any(), any(), anyBoolean(), any(), any())).thenAnswer(
                (Answer<Callable<ExecutionPlan>>) invocation -> () -> {
                    ExecutionPlan plan = Mockito.mock(ExecutionPlan.class);
                    Mockito.when(plan.getPlan()).thenReturn(va1);
                    return plan;
                }
            );

            AtomicInteger count = new AtomicInteger(0);
            Mockito.when(planCache.getPlanWithLoader(any(), any())).thenAnswer(
                (Answer<ExecutionPlan>) invocation -> {
                    count.getAndIncrement();
                    if (count.get() == 1) {
                        ExecutionPlan plan = Mockito.mock(ExecutionPlan.class);
                        Mockito.when(plan.getPlan()).thenReturn(va0);
                        return plan;
                    }
                    Object[] args = invocation.getArguments();
                    Callable<ExecutionPlan> loader = (Callable<ExecutionPlan>) args[1];
                    return loader.call();
                });

            Mockito.when(planCache.savePlanCachedKey(any(), any(), any(), any())).thenAnswer(
                (Answer<ExecutionPlan>) invocation -> (ExecutionPlan) invocation.getArguments()[1]);

            Mockito.when(planCache.checkColumnarPlanCache(any(), any(), any(), any(), any(), any())).thenAnswer(
                invocation -> invocation.getArgument(5));

            doCallRealMethod().when(planCache).checkColumnarDisable(any(), any(), any(), any(), any());
            doCallRealMethod().when(planCache).getFromCache(any(), any(), any(), any(), anyBoolean());
            ExecutionPlan plan = planCache.getFromCache("test", sqlParameterized, null, executionContext, false);

            // row plan cached
            if (!cacheColumnar) {
                // plan must be row
                Assert.check(!PlannerContext.getPlannerContext(plan.getPlan()).isUseColumnar());
                Assert.check(count.get() == 1);
                return;
            }

            // cacheColumnar == true
            if (enableColumnar) {
                Assert.check(PlannerContext.getPlannerContext(plan.getPlan()).isUseColumnar());
                Assert.check(count.get() == 1);
            }

            if (!enableColumnar) {
                Assert.check(!PlannerContext.getPlannerContext(plan.getPlan()).isUseColumnar());
                Assert.check(count.get() == 2);
            }
        } finally {
            DynamicConfig.getInstance().existColumnarNodes(false);
        }
    }

}
