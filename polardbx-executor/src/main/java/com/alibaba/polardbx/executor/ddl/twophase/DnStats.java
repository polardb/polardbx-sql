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

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoAccessor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.gms.topology.DbTopologyManager.getGroupNameToStorageInstIdMap;

public class DnStats {
    private final static Logger LOG = SQLRecorderLogger.ddlLogger;

    String storageInstId;
    int maxConnection;
    int maxUserConnection;
    int connection;

    Long jobId;

    public DnStats(String storageInstId, int maxConnection, int maxUserConnection, int connection, Long jobId) {
        this.storageInstId = storageInstId;
        this.maxConnection = maxConnection;
        this.maxUserConnection = maxUserConnection;
        this.connection = connection;
        this.jobId = jobId;
    }

    public int getResidueConnection() {
        if (maxUserConnection == 0) {
            return maxConnection - connection;
        } else {
            return Math.min(maxConnection, maxUserConnection) - connection;
        }
    }

    public Boolean checkConnectionNum(int requiredConnectionNum) {
        int residueConnectionNum = getResidueConnection();
        Boolean result =
            (residueConnectionNum * 4 / 5 > requiredConnectionNum) && (residueConnectionNum - requiredConnectionNum
                > 32);
        String logInfo = String.format(
            "<MultiPhaseDdl %d> check dn %s, max_connection=%d, max_user_connection=%d, used_connection=%d, require_connection=%d, check %s",
            jobId, storageInstId, maxConnection, maxUserConnection, connection, requiredConnectionNum,
            result);
        LOG.info(logInfo);
        return result;
    }

    public static Map<String, String> buildGroupToDnMap(String schemaName, String tableName,
                                                        ExecutionContext executionContext) {
        return getGroupNameToStorageInstIdMap(schemaName);
    }

    public static Map<String, DnStats> buildDnStats(String schemaName, String tableName,
                                                    Map<String, String> groupToDnMap,
                                                    Long jobId,
                                                    ExecutionContext executionContext) {
        String taskName = "TWO_PHASE_DDL_INIT_TASK";
        Map<String, String> dnToGroupMap =
            groupToDnMap.keySet().stream()
                .collect(Collectors.toMap(groupToDnMap::get, o -> o, (before, after) -> after));
        String showMaxConnectionSql = "show global variables like 'max_connections';";
        String showMaxUserConnectionSql = "show global variables like 'max_user_connections';";
        String showConnectionStatusSql = "show global status like 'Threads_connected';";
        ConcurrentHashMap<String, DnStats> results = new ConcurrentHashMap<>();
        dnToGroupMap.keySet().forEach(dn -> {
                String groupName = dnToGroupMap.get(dn);
                int maxConnection =
                    Integer.parseInt(
                        TwoPhaseDdlUtils.queryGroupBypassConnPool(executionContext, jobId, taskName, schemaName, tableName,
                            groupName,
                            showMaxConnectionSql).get(0).get("Value").toString());
                int maxUserConnection =
                    Integer.parseInt(
                        TwoPhaseDdlUtils.queryGroupBypassConnPool(executionContext, jobId, taskName, schemaName, tableName,
                            groupName,
                            showMaxUserConnectionSql).get(0).get("Value").toString());
                int connection =
                    Integer.parseInt(
                        TwoPhaseDdlUtils.queryGroupBypassConnPool(executionContext, jobId, taskName, schemaName, tableName,
                            groupName,
                            showConnectionStatusSql).get(0).get("Value").toString());
                results.put(dn, new DnStats(dn, maxConnection, maxUserConnection, connection, jobId));
            }
        );
        return results;
    }

}
