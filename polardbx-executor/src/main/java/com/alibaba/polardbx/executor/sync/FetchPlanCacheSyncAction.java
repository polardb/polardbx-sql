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

package com.alibaba.polardbx.executor.sync;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.PlaceHolderExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.PlanCache;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author dylan
 */
public class FetchPlanCacheSyncAction implements ISyncAction {

    private String schemaName;

    private boolean withPlan;

    private boolean withParameter;

    public FetchPlanCacheSyncAction(String schemaName) {
        this.schemaName = schemaName;
        this.withPlan = true;
        this.withParameter = false;
    }

    public FetchPlanCacheSyncAction(String schemaName, boolean withPlan) {
        this.schemaName = schemaName;
        this.withPlan = withPlan;
        this.withParameter = false;
    }

    public FetchPlanCacheSyncAction(String schemaName, boolean withPlan, boolean withParameter) {
        this.schemaName = schemaName;
        this.withPlan = withPlan;
        this.withParameter = withParameter;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public boolean isWithPlan() {
        return withPlan;
    }

    public void setWithPlan(boolean withPlan) {
        this.withPlan = withPlan;
    }

    public boolean isWithParameter() {
        return withParameter;
    }

    public void setWithParameter(boolean withParameter) {
        this.withParameter = withParameter;
    }

    @Override
    public ResultCursor sync() {
        ArrayResultCursor result = new ArrayResultCursor("PLAN_CACHE");
        result.addColumn("COMPUTE_NODE", DataTypes.StringType);
        result.addColumn("TABLE_NAMES", DataTypes.StringType);
        result.addColumn("ID", DataTypes.StringType);
        result.addColumn("HIT_COUNT", DataTypes.LongType);
        result.addColumn("SQL", DataTypes.StringType);
        result.addColumn("TYPE_DIGEST", DataTypes.LongType);
        result.addColumn("PLAN", DataTypes.StringType);
        result.addColumn("PARAMETER", DataTypes.StringType);

        for (Map.Entry<PlanCache.CacheKey, ExecutionPlan> entry : PlanCache.getInstance().getCache().asMap()
            .entrySet()) {
            PlanCache.CacheKey cacheKey = entry.getKey();
            ExecutionPlan executionPlan = entry.getValue();
            final String plan;
            if (withPlan) {
                if (executionPlan == PlaceHolderExecutionPlan.INSTANCE) {
                    plan = "MISS";
                } else {
                    plan = "\n" + RelOptUtil.dumpPlan("",
                        executionPlan.getPlan(),
                        SqlExplainFormat.TEXT,
                        SqlExplainLevel.NO_ATTRIBUTES);
                }
            } else {
                plan = null;
            }
            final String parameter;
            if (withParameter) {
                if (cacheKey.getParameters() == null) {
                    parameter = null;
                } else {
                    parameter = JSON.toJSONString(cacheKey.getParameters());
                }
            } else {
                parameter = null;
            }

            result.addRow(new Object[] {
                TddlNode.getHost() + ":" + TddlNode.getPort(),
                cacheKey.getTableMetas().stream().map(TableMeta::getTableName).collect(Collectors.joining(",")),
                cacheKey.getTemplateId(),
                executionPlan.getHitCount().longValue(),
                cacheKey.getParameterizedSql(),
                cacheKey.getTypeDigest(),
                plan,
                parameter
            });
        }

        return result;
    }
}

