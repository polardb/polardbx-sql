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

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.parse.SqlParameterizeUtils;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.alibaba.polardbx.planner.common.PlanTestCommon;
import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.List;
import java.util.concurrent.ExecutionException;

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
}
