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

package com.alibaba.polardbx.optimizer.sharding.advisor;

import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.utils.ExplainResult;
import com.alibaba.polardbx.planner.common.EclipseParameterized;
import com.alibaba.polardbx.planner.common.PlanTestCommon;
import org.apache.calcite.rel.RelNode;
import org.junit.Ignore;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author shengyu
 */
@Ignore
public class ShardingAdvisorTestCommon extends PlanTestCommon {

    public ShardingAdvisorTestCommon(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    public static List<Object[]> loadSqls(Class clazz) {
        List<Object[]> sqls = PlanTestCommon.loadSqls(clazz);
        return new ArrayList<>(Collections.singleton(sqls.get(0)));
    }

    public void doPlanTest() {
        getPlan(this.caseName, this.expectedPlan);
    }

    protected String getPlan(String testMethodName, String targetPlan) {
        ExecutionContext executionContext = new ExecutionContext();
        executionContext.setInternalSystemSql(false);

        executionContext.setParams(new Parameters());
        executionContext.getExtraCmds().put(ConnectionProperties.PARALLELISM, enableParallelQuery ? -1 : 0);
        executionContext.getExtraCmds().put(ConnectionProperties.ENABLE_JOIN_CLUSTERING, enableJoinClustering);
        executionContext.getExtraCmds().put(ConnectionProperties.ENABLE_MPP, enableMpp);
        executionContext.getExtraCmds().put(ConnectionProperties.STORAGE_SUPPORTS_BLOOM_FILTER,
            storageSupportsBloomFilter);
        executionContext.getExtraCmds().put(ConnectionProperties.ENABLE_PUSH_JOIN,
            false);

        if (inValuesThread > 1) {
            executionContext.getExtraCmds().put(ConnectionProperties.IN_SUB_QUERY_THRESHOLD, inValuesThread);
            executionContext.setSqlType(sqlType);
        }

        if (partialAggBucketThreshold != -1) {
            executionContext.getExtraCmds().put(ConnectionProperties.PARTIAL_AGG_BUCKET_THRESHOLD,
                partialAggBucketThreshold);
        }
        ExplainResult explainResult = new ExplainResult();
        explainResult.explainMode = ExplainResult.ExplainMode.ADVISOR;
        executionContext.setExplain(explainResult);

        PlannerContext plannerContext = PlannerContext.fromExecutionContext(executionContext);
        plannerContext.setSchemaName(appName);
        executionContext.setServerVariables(new HashMap<>());
        for (int i = 0; i < 1; i++) {
            Class clazz = this.getClass();
            URL url = clazz.getResource(clazz.getSimpleName() + ".class");
            File dir = new File(url.getPath().substring(0, url.getPath().indexOf(clazz.getSimpleName() + ".class")));
            File[] filesList = dir.listFiles();
            List<String> fileNames = new ArrayList<>();
            for (File file : filesList) {
                if (file.isFile()) {
                    if (file.getName().startsWith(clazz.getSimpleName() + ".") && file.getName().endsWith(".yml")) {
                        if (file.getName().equals(clazz.getSimpleName() + ".ddl.yml")
                            || file.getName().equals(clazz.getSimpleName() + ".outline.yml")
                            || file.getName().equals(clazz.getSimpleName() + ".statistic.yml")) {
                            continue;
                        }
                        fileNames.add(file.getPath());
                    }
                }
            }

            ShardingAdvisor shardingAdvisor = new ShardingAdvisor();
            ShardResultForOutput adviceResult = shardingAdvisor.advise(plannerContext, fileNames);
            String realPlan = adviceResult.display().get(appName).toString();
            System.out.println("Running test " + testMethodName + " - " + sqlIndex);
            System.out.println("link: xx.xx(" + testMethodName + ":" + lineNum + ")");
            assertEquals("\nrealShardingPlanVal = \n" + realPlan
                + "\targetPlanVal = \n" + targetPlan + "\n", realPlan, targetPlan);
        }
        return null;
    }
}
