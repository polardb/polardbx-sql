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
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.alibaba.polardbx.common.utils.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.alibaba.polardbx.common.utils.GeneralUtil.unixTimeStamp;

public class BaselineInfo {
    private int id;

    private String parameterSql; // unique key

    private Set<Pair<String, String>> tableSet;

    // planInfoId -> PlanInfo
    private Map<Integer, PlanInfo> acceptedPlans = new ConcurrentHashMap<>();

    // planInfoId -> PlanInfo
    private Map<Integer, PlanInfo> unacceptedPlans = new ConcurrentHashMap<>();

    // use for evolution fail plan, just use hashCode to save memory
    private Set<Integer> evolutionFailPlanHashSet = ConcurrentHashMap.newKeySet();

    private boolean dirty = false;

    private String extend;

    private BaselineInfo() {
    }

    public BaselineInfo(String parameterSql, Set<Pair<String, String>> tableSet) {
        this.parameterSql = parameterSql;
        this.id = parameterSql.hashCode();
        this.tableSet = tableSet;
    }

    public int getId() {
        return id;
    }

    public Set<Pair<String, String>> getTableSet() {
        return tableSet;
    }

    public void addAcceptedPlan(PlanInfo planInfo) {
        planInfo.setAccepted(true);
        acceptedPlans.put(planInfo.getId(), planInfo);
    }

    public void addUnacceptedPlan(PlanInfo planInfo) {
        planInfo.setAccepted(false);
        unacceptedPlans.put(planInfo.getId(), planInfo);
    }

    public void removeAcceptedPlan(Integer planInfoId) {
        acceptedPlans.remove(planInfoId);
    }

    public void removeUnacceptedPlan(Integer planInfoId) {
        unacceptedPlans.remove(planInfoId);
    }

    public Map<Integer, PlanInfo> getAcceptedPlans() {
        return acceptedPlans;
    }

    public Map<Integer, PlanInfo> getUnacceptedPlans() {
        return unacceptedPlans;
    }

    public String getParameterSql() {
        return parameterSql;
    }

    public boolean isDirty() {
        return dirty;
    }

    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    public void addEvolutionFailPlan(int planInfoId) {
        evolutionFailPlanHashSet.add(planInfoId);
    }

    public boolean evolutionFailPlanHashSetContain(int planInfoId) {
        return evolutionFailPlanHashSet.contains(planInfoId);
    }

    public boolean tooLongNoUsed() {
        for (PlanInfo planInfo : acceptedPlans.values()) {
            long lastTime =
                planInfo.getLastExecuteTime() != null ? planInfo.getLastExecuteTime() : planInfo.getCreateTime();
            if (unixTimeStamp() - lastTime < 7 * 24 * 60 * 60) {
                return false;
            }
        }
        // a week no used
        return true;
    }

    public void removeInvalidUnacceptedPlans() {
        List<PlanInfo> invalidatePlanInfo = new ArrayList<>();
        for (PlanInfo planInfo : unacceptedPlans.values()) {
            long lastTime =
                planInfo.getLastExecuteTime() != null ? planInfo.getLastExecuteTime() : planInfo.getCreateTime();
            if (unixTimeStamp() - lastTime > 24 * 60 * 60) {
                invalidatePlanInfo.add(planInfo);
            }
        }
        for (PlanInfo planInfo : invalidatePlanInfo) {
            removeUnacceptedPlan(planInfo.getId());
        }
    }

    public static String serializeToJson(BaselineInfo baselineInfo) {
        JSONObject baselineInfoJson = new JSONObject();
        baselineInfoJson.put("id", baselineInfo.getId());
        baselineInfoJson.put("parameterSql", baselineInfo.getParameterSql());
        baselineInfoJson.put("tableSet", serializeTableSet(baselineInfo.getTableSet()));

        Map<String, String> acceptedPlansMap = new HashMap<>();
        for (Map.Entry<Integer, PlanInfo> entry : baselineInfo.getAcceptedPlans().entrySet()) {
            Integer planInfoId = entry.getKey();
            PlanInfo planInfo = entry.getValue();
            acceptedPlansMap.put(planInfoId.toString(), PlanInfo.serializeToJson(planInfo));
        }
        baselineInfoJson.put("acceptedPlans", acceptedPlansMap);

        Map<String, String> unacceptedPlansMap = new HashMap<>();
        for (Map.Entry<Integer, PlanInfo> entry : baselineInfo.getUnacceptedPlans().entrySet()) {
            Integer planInfoId = entry.getKey();
            PlanInfo planInfo = entry.getValue();
            acceptedPlansMap.put(planInfoId.toString(), PlanInfo.serializeToJson(planInfo));
        }
        baselineInfoJson.put("unacceptedPlans", unacceptedPlansMap);

        return baselineInfoJson.toJSONString();
    }

    /**
     * base info , just for log
     * @param baselineInfo
     * @return
     */
    public static String serializeBaseInfoToJson(BaselineInfo baselineInfo) {
        JSONObject baselineInfoJson = new JSONObject();
        baselineInfoJson.put("id", baselineInfo.getId());
        baselineInfoJson.put("parameterSql", baselineInfo.getParameterSql());
        baselineInfoJson.put("tableSet", serializeTableSet(baselineInfo.getTableSet()));
        baselineInfoJson.put("extends", baselineInfo.getExtend());

        return baselineInfoJson.toJSONString();
    }

    public static BaselineInfo deserializeFromJson(String json) {
        JSONObject baselineInfoJson = JSON.parseObject(json);
        BaselineInfo baselineInfo = new BaselineInfo();
        baselineInfo.id = baselineInfoJson.getIntValue("id");
        baselineInfo.parameterSql = baselineInfoJson.getString("parameterSql");
        baselineInfo.tableSet = deserializeTableSet(baselineInfoJson.getString("tableSet"));

        Map<Integer, PlanInfo> acceptedPlans = new ConcurrentHashMap<>();
        JSONObject acceptedPlansJsonObject = baselineInfoJson.getJSONObject("acceptedPlans");
        for (Map.Entry<String, Object> entry : acceptedPlansJsonObject.entrySet()) {
            acceptedPlans.put(Integer.valueOf(entry.getKey()),
                PlanInfo.deserializeFromJson(acceptedPlansJsonObject.getString(entry.getKey())));
        }
        baselineInfo.acceptedPlans = acceptedPlans;

        Map<Integer, PlanInfo> unacceptedPlans = new ConcurrentHashMap<>();
        JSONObject unacceptedPlansJsonObject = baselineInfoJson.getJSONObject("unacceptedPlans");
        for (Map.Entry<String, Object> entry : unacceptedPlansJsonObject.entrySet()) {
            unacceptedPlans.put(Integer.valueOf(entry.getKey()),
                PlanInfo.deserializeFromJson(unacceptedPlansJsonObject.getString(entry.getKey())));
        }
        baselineInfo.unacceptedPlans = unacceptedPlans;

        return baselineInfo;
    }

    public static String serializeTableSet(Set<Pair<String, String>> tableSet) {
        return JSON.toJSONString(tableSet);
    }

    public static Set<Pair<String, String>> deserializeTableSet(String jsonString) {
        Set<Pair<String, String>> tableSet = new HashSet<>();
        JSONArray jsonArray = JSON.parseArray(jsonString);
        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject o = jsonArray.getJSONObject(i);
            tableSet.add(Pair.of(o.getString("key"), o.getString("value")));
        }
        return tableSet;
    }

    public String getExtend() {
        return extend;
    }

    public void setExtend(String extend) {
        this.extend = extend;
    }

    public PlanInfo getPlan(int planId) {
        PlanInfo planInfo = acceptedPlans.get(planId);
        if (planInfo != null) {
            return planInfo;
        }
        planInfo = unacceptedPlans.get(planId);
        return planInfo;
    }

    public List<PlanInfo> getPlans() {
        List<PlanInfo> planInfos = Lists.newArrayList();
        planInfos.addAll(getAcceptedPlans().values());
        planInfos.addAll(getUnacceptedPlans().values());
        return planInfos;
    }
}
