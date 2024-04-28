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

package com.alibaba.polardbx.executor.ddl.twophase;

import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import org.apache.calcite.util.Pair;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TwoPhaseDdlData {
    public static Boolean initialPhyDdlSuccess(List<List<Object>> results) {
        Boolean succeed = results.stream().allMatch(o -> o.get(1).toString().equals("1"));
        return succeed;
    }

    public static Boolean evolvePhyDdlSuccess(List<List<Object>> results) {
        Boolean succeed = results.stream().allMatch(o -> o.get(0).toString().equals("1"));
        return succeed;
    }

    public static String REACHED_BARRIER = "REACHED_BARRIER";
    public static String REACHED_BARRIER_RUNNING = "REACHED_BARRIER_RUNNING";
    public static String RUNNING = "RUNNING";

    public static Long REACHED_BARRIER_STATE = 1L;

    public static Long NON_REACHED_BARRIER_STATE = 0L;

    // physicalDbName / physicalTable
    // phase
    // state
    // queryId
    public static Map<String, Long> resultsToQueryIdMap(List<Map<String, Object>> results) {
        Map<String, Long> queryIdMap = new HashMap<>();
        for (Map<String, Object> row : results) {
            String physicalDbTableName = (String) row.get("PHYSICAL_TABLE");
            Long queryId = (Long) row.get("PROCESS_ID");
            queryIdMap.put(physicalDbTableName, queryId);
        }
        return queryIdMap;
    }
    // physicalDbName / physicalTable
    // phase
    // state
    // queryId

    public static Map<String, String> resultsToPhysicalDdlStateMap(List<Map<String, Object>> results) {
        Map<String, String> physicalDdlRunningStateMap = new HashMap<>();
        for (Map<String, Object> row : results) {
            String physicalDbTableName = (String) (row.get("PHYSICAL_TABLE"));
            String phyDdlState = (String) (row.get("STATE"));
            physicalDdlRunningStateMap.put(physicalDbTableName, phyDdlState);
        }
        return physicalDdlRunningStateMap;
    }

    public static String resultsToPhysicalDdlPhase(List<Map<String, Object>> results) {
        if (results.isEmpty()) {
            return "FINISH";
        } else {
            return (String) (results.get(0).get("PHASE"));
        }
    }

    public static Map<Pair<String, Long>, String> resultsToQueryIdToProcessInfoMap(List<Map<String, Object>> results) {
        Map<Pair<String, Long>, String> queryIdToProcessInfoMap = new HashMap<>();
        for (Map<String, Object> row : results) {
            Long processId = (Long) (row.get("Id"));
            String dbName = (String) (row.get("db"));
            String processInfo = (String) (row.get("Info"));
            // While check finished, the thread pool in dn may cause thread hang for a long time before return.
            if (StringUtils.equalsIgnoreCase(processInfo, "NULL")) {
                processInfo = null;
            }
            queryIdToProcessInfoMap.put(Pair.of(dbName, processId), processInfo);
        }
        return queryIdToProcessInfoMap;
    }

    public static Map<String, Long> resultsToStateMap(List<Map<String, Object>> results) {
        Map<String, Long> phyTableDdlState = new HashMap<>();
        for (Map<String, Object> row : results) {
            String physicalDbTableName = (String) (row.get("PHYSICAL_TABLE"));
            String state = (String) (row.get("STATE"));
            if (state.equalsIgnoreCase(REACHED_BARRIER)) {
                phyTableDdlState.put(physicalDbTableName, REACHED_BARRIER_STATE);
            } else {
                phyTableDdlState.put(physicalDbTableName, NON_REACHED_BARRIER_STATE);
            }
        }
        return phyTableDdlState;
    }

    public static Boolean resultsToFinishSuccess(List<Map<String, Object>> results) {
        String firstColumnName = results.get(0).keySet().stream().findFirst().get();
        Long result = (Long) (results.get(0).get(firstColumnName));
        return result == 1;
    }
}
