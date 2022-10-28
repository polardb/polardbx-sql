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

package com.alibaba.polardbx.optimizer.planmanager;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.google.common.collect.Maps;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.JsonBuilder;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static com.alibaba.polardbx.common.utils.GeneralUtil.unixTimeStamp;

public class PlanInfo {

    public static int INVAILD_HASH_CODE = -1;

    private int id;

    private int baselineId;

    private byte[] compressPlanByteArray; // unique key

    private int chooseCount = 0;

    private boolean accepted;

    private boolean fixed;

    private double cost;

    private String traceId;

    private String origin;

    private String extend;

    private long createTime; // unix time

    private Long lastExecuteTime; // unix time, default null

    private double estimateExecutionTime = -1; // estimation of execution time, in seconds

    private RelNode plan = null;

    private int tablesHashCode;

    private AtomicInteger errorCount = new AtomicInteger(0);

    /**
     * params from extend
     */
    private String fixHint;

    private PlanInfo() {
    }

    public PlanInfo(String planJsonString, int baselineId, double cost, String traceId, String origin,
                    int tablesHashCode) {
        this.compressPlanByteArray = PlanManagerUtil.compressPlan(planJsonString);
        this.id = planJsonString.hashCode();
        this.baselineId = baselineId;
        this.createTime = unixTimeStamp();
        this.cost = cost;
        this.traceId = traceId;
        this.origin = origin;
        this.tablesHashCode = tablesHashCode;
    }

    public PlanInfo(RelNode plan, int baselineId, double cost, String traceId, String origin,
                    int tablesHashCode) {
        String planJson = PlanManagerUtil.relNodeToJson(plan);
        this.compressPlanByteArray = PlanManagerUtil.compressPlan(planJson);
        this.plan = plan;
        this.id = planJson.hashCode();
        this.baselineId = baselineId;
        this.createTime = unixTimeStamp();
        this.cost = cost;
        this.traceId = traceId;
        this.origin = origin;
        this.tablesHashCode = tablesHashCode;
    }

    public PlanInfo(int baselineId, String planJsonString, long createTime, Long lastExecuteTime, int chooseCount,
                    double cost, double estimateExecutionTime, boolean accepted, boolean fixed, String traceId,
                    String origin, String extend, int tablesHashCode) {
        this.compressPlanByteArray = PlanManagerUtil.compressPlan(planJsonString);
        this.id = planJsonString.hashCode();
        this.baselineId = baselineId;
        this.createTime = createTime;
        this.lastExecuteTime = lastExecuteTime;
        this.chooseCount = chooseCount;
        this.cost = cost;
        this.estimateExecutionTime = estimateExecutionTime;
        this.accepted = accepted;
        this.fixed = fixed;
        this.traceId = traceId;
        this.origin = origin;
        this.extend = extend;
        this.tablesHashCode = tablesHashCode;

        decodeExtend();
    }

    private void decodeExtend() {
        if (extend == null || "".equals(extend)) {
            return;
        }
        Map<String, Object> extendMap = (Map<String, Object>) JSON.parseObject(extend);
        Object tmpFixHint = extendMap.get("FIX_HINT");
        if (tmpFixHint != null) {
            fixHint = tmpFixHint.toString();
        }
    }

    public String encodeExtend() {
        if (fixHint == null || "".equals(fixHint)) {
            return "";
        }
        final JsonBuilder jsonBuilder = new JsonBuilder();

        Map<String, Object> extendMap = Maps.newHashMap();
        extendMap.put("FIX_HINT", fixHint);
        return jsonBuilder.toJsonString(extendMap);
    }

    public int getId() {
        return id;
    }

    public int getBaselineId() {
        return baselineId;
    }

    public String getTraceId() {
        return traceId;
    }

    public String getPlanJsonString() {
        return PlanManagerUtil.uncompressPlan(compressPlanByteArray);
    }

    public void addChooseCount() {
        chooseCount++;
    }

    public int getChooseCount() {
        return chooseCount;
    }

    public boolean isAccepted() {
        return accepted;
    }

    public void setAccepted(boolean accepted) {
        this.accepted = accepted;
    }

    public void setFixed(boolean fixed) {
        this.fixed = fixed;
    }

    public boolean isFixed() {
        return fixed;
    }

    public double getCost() {
        return cost;
    }

    public long getCreateTime() {
        return createTime;
    }

    public Long getLastExecuteTime() {
        return lastExecuteTime;
    }

    public void setLastExecuteTime(long lastExecuteTime) {
        this.lastExecuteTime = lastExecuteTime;
    }

    public double getEstimateExecutionTime() {
        return estimateExecutionTime;
    }

    public String getOrigin() {
        return origin;
    }

    public void setOrigin(String origin) {
        this.origin = origin;
    }

    synchronized public void updateEstimateExecutionTime(double lastExecutionTimeInSeconds) {
        if (estimateExecutionTime == -1) {
            estimateExecutionTime = lastExecutionTimeInSeconds;
        } else {
            estimateExecutionTime = estimateExecutionTime * 0.8 + lastExecutionTimeInSeconds * 0.2;
        }
    }

    public void resetPlan(RelNode newPlan) {
        this.compressPlanByteArray = PlanManagerUtil.compressPlan(PlanManagerUtil.relNodeToJson(newPlan));
        this.plan = newPlan;
    }

    public RelNode getPlan(RelOptCluster cluster, RelOptSchema relOptSchema) {
        if (plan == null) {
            if (cluster == null || relOptSchema == null) {
                return null;
            }
            synchronized (this) {
                if (plan == null) {
                    plan = PlanManagerUtil
                        .jsonToRelNode(getPlanJsonString(), cluster, relOptSchema);
                    // clear plannerContext cost cache
                    PlannerContext.getPlannerContext(plan).setCost(null);
                }
            }
        }
        return plan;
    }

    public boolean inited() {
        return plan != null;
    }

    synchronized RelOptCost getCumulativeCost(RelOptCluster cluster, RelOptSchema relOptSchema, Parameters parameters) {
        RelNode plan = getPlan(cluster, relOptSchema);
        PlannerContext.getPlannerContext(plan).setParams(parameters);
        return cluster.getMetadataQuery().getCumulativeCost(plan);
    }

    public int incrementAndGetErrorCount() {
        return errorCount.incrementAndGet();
    }

    public void zeroErrorCount() {
        errorCount.set(0);
    }

    public static String serializeToJson(PlanInfo planInfo) {
        JSONObject planInfoJson = new JSONObject();
        planInfoJson.put("id", planInfo.getId());
        planInfoJson.put("baselineId", planInfo.getBaselineId());
        planInfoJson.put("traceId", planInfo.getTraceId());
        planInfoJson.put("planJsonString", planInfo.getPlanJsonString());
        planInfoJson.put("hashcode", planInfo.getTablesHashCode());
        planInfoJson.put("chooseCount", planInfo.getChooseCount());
        planInfoJson.put("cost", planInfo.getCost());
        planInfoJson.put("fixed", planInfo.isFixed());
        planInfoJson.put("accepted", planInfo.isAccepted());
        planInfoJson.put("createTime", planInfo.getCreateTime());
        planInfoJson.put("lastExecuteTime", planInfo.getLastExecuteTime());
        planInfoJson.put("estimateExecutionTime", planInfo.getEstimateExecutionTime());
        planInfoJson.put("origin", planInfo.getOrigin());
        planInfoJson.put("extend", planInfo.encodeExtend());
        return planInfoJson.toJSONString();
    }

    public static PlanInfo deserializeFromJson(String json) {
        JSONObject planInfoJson = JSON.parseObject(json);
        PlanInfo planInfo = new PlanInfo();
        planInfo.id = planInfoJson.getIntValue("id");
        planInfo.baselineId = planInfoJson.getIntValue("baselineId");
        planInfo.traceId = planInfoJson.getString("traceId");
        planInfo.compressPlanByteArray = PlanManagerUtil.compressPlan(planInfoJson.getString("planJsonString"));
        planInfo.chooseCount = planInfoJson.getIntValue("chooseCount");
        planInfo.cost = planInfoJson.getDoubleValue("cost");
        planInfo.fixed = planInfoJson.getBooleanValue("fixed");
        planInfo.accepted = planInfoJson.getBooleanValue("accepted");
        planInfo.createTime = planInfoJson.getLongValue("createTime");
        planInfo.lastExecuteTime = planInfoJson.getLong("lastExecuteTime");
        planInfo.estimateExecutionTime = planInfoJson.getDoubleValue("estimateExecutionTime");
        planInfo.origin = planInfoJson.getString("origin");
        planInfo.extend = planInfoJson.getString("extend");
        try {
            planInfo.tablesHashCode = planInfoJson.getInteger("hashcode");
        } catch (Throwable t) {
            planInfo.tablesHashCode = INVAILD_HASH_CODE;
        }

        return planInfo;
    }

    public int getTablesHashCode() {
        return tablesHashCode;
    }

    public void setTablesHashCode(int tablesHashCode) {
        this.tablesHashCode = tablesHashCode;
    }

    public String getExtend() {
        return extend;
    }

    public void setExtend(String extend) {
        this.extend = extend;
    }

    public String getFixHint() {
        return fixHint;
    }

    public void setFixHint(String fixHint) {
        this.fixHint = fixHint;
    }
}
