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

package com.alibaba.polardbx.executor.scaleout;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.LongConfigParam;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.thread.ThreadCpuStatUtil;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.ha.impl.StorageInstHaContext;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.topology.DbGroupInfoAccessor;
import com.alibaba.polardbx.gms.topology.DbGroupInfoRecord;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.optimizer.config.table.ScaleOutPlanUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;

import java.sql.Connection;
import java.text.MessageFormat;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class ScaleOutUtils {

    public static final int RETRY_COUNT = 3;
    public static final long[] RETRY_WAIT = new long[RETRY_COUNT];

    static {
        IntStream.range(0, RETRY_COUNT).forEach(i -> RETRY_WAIT[i] = Math.round(Math.pow(2, i)));
    }

    public static void cleanUpUselessGroups(Map<String, List<String>> storageGroups, String schemaName,
                                            ExecutionContext executionContext) {
        for (Map.Entry<String, List<String>> entry : storageGroups.entrySet()) {
            for (String groupName : entry.getValue()) {
                SQLRecorderLogger.scaleOutTaskLogger.info(MessageFormat.format(
                    "start to clean up the useless group [{0}], ts={1} ", groupName,
                    String.valueOf(Instant.now().toEpochMilli())));
                Long socketTimeout = executionContext.getParamManager().getLong(ConnectionParams.SOCKET_TIMEOUT);
                long socketTimeoutVal = socketTimeout != null ? socketTimeout : -1;
                boolean dropDatabaseAfterSwitch =
                    ScaleOutPlanUtil.dropOldDatabaseAfterSwitchDatasource(executionContext.getParamManager());
                DbTopologyManager.removeScaleOutGroupFromDb(schemaName, groupName, false, true, socketTimeoutVal,
                    dropDatabaseAfterSwitch);
                SQLRecorderLogger.scaleOutTaskLogger.info(MessageFormat.format(
                    "finish to clean up the useless group [{0}], ts={1} ", groupName,
                    String.valueOf(Instant.now().toEpochMilli())));
            }
        }
    }

    public static void doAddNewGroupIntoDb(String schemaName, String targetGroup,
                                           String phyDbOfTargetGroup,
                                           String targetStorageInstId,
                                           Logger logger,
                                           Connection metaDbConnection) {
        // Check if new added group can be access
        Throwable ex = null;
        int tryCnt = 0;
        do {
            try {
                DbTopologyManager
                    .addNewGroupIntoDb(schemaName, targetStorageInstId, targetGroup,
                        phyDbOfTargetGroup, true, metaDbConnection);
                ex = null;
                break;
            } catch (Throwable e) {
                ex = e;
                tryCnt++;
                String errMsg = String
                    .format("Failed to add new group[%s] to db[%s], tryCnt is %s, err is [%s]", targetGroup,
                        schemaName, tryCnt, ex.getMessage());
                logger.warn(errMsg, ex);
            }
        } while (tryCnt < 3);
        if (ex != null) {
            throw new TddlRuntimeException(ErrorCode.ERR_SCALEOUT_EXECUTE,
                String.format("Failed to add new group[%s] to db[%s], err is [%s]", targetGroup, schemaName,
                    ex.getMessage()), ex);
        }
    }

    public static void doRemoveNewGroupFromDb(String schemaName,
                                              String targetGroup,
                                              String phyDbOfTargetGroup,
                                              String targetStorageInstId,
                                              Long socketTimeOut,
                                              Logger logger,
                                              Connection metaDbConnection) {
        Throwable ex = null;
        int tryCnt = 0;
        do {
            try {
                DbTopologyManager
                    .removeOldGroupFromDb(schemaName, targetStorageInstId, targetGroup,
                        phyDbOfTargetGroup, socketTimeOut, metaDbConnection);
                ex = null;
                break;
            } catch (Throwable e) {
                ex = e;
                tryCnt++;
                String errMsg = String
                    .format("Failed to remove the group[%s] from db[%s], tryCnt is %s, err is [%s]", targetGroup,
                        schemaName, tryCnt, ex.getMessage());
                logger.warn(errMsg, ex);
            }
        } while (tryCnt < 3);
        if (ex != null) {
            throw new TddlRuntimeException(ErrorCode.ERR_SCALEOUT_EXECUTE,
                String.format("Failed to remove the group[%s] from db[%s], err is [%s]", targetGroup, schemaName,
                    ex.getMessage()), ex);
        }
    }

    public static void updateGroupType(String schemaName,
                                       String groupName,
                                       int groupType,
                                       Connection metaDbConn) {
        updateGroupType(schemaName, Arrays.asList(groupName), groupType, metaDbConn);
    }

    /**
     * Update group type and notify some registered listeners
     */
    public static void updateGroupType(String schemaName,
                                       List<String> groupNameList,
                                       int groupType,
                                       Connection metaDbConn) {
        updateGroupType(schemaName, groupNameList, -1, groupType, metaDbConn);
    }

    public static void updateGroupType(String schemaName,
                                       List<String> groupNameList,
                                       int beforeGroupType,
                                       int afterGroupType,
                                       Connection metaDbConn) {
        DbGroupInfoAccessor dbGroupInfoAccessor = new DbGroupInfoAccessor();
        dbGroupInfoAccessor.setConnection(metaDbConn);

        for (String groupName : groupNameList) {
            if (beforeGroupType >= 0) {
                DbGroupInfoRecord
                    dbGroupInfoRecord =
                    dbGroupInfoAccessor.getDbGroupInfoByDbNameAndGroupName(schemaName, groupName, false);
                if (dbGroupInfoRecord.groupType != beforeGroupType) {
                    continue;
                }
            }
            // update metaDb
            dbGroupInfoAccessor.updateGroupTypeByDbAndGroup(schemaName, groupName, afterGroupType);

            // group.config
            String instIdOfGroup = InstIdUtil.getInstId();
            String groupConfigDataId = MetaDbDataIdBuilder.getGroupConfigDataId(instIdOfGroup, schemaName, groupName);
            MetaDbConfigManager.getInstance().notify(groupConfigDataId, metaDbConn);
        }

        // db.topology
        String dbTopologyDataId = MetaDbDataIdBuilder.getDbTopologyDataId(schemaName);
        MetaDbConfigManager.getInstance().notify(dbTopologyDataId, metaDbConn);
    }

    /**
     * Parallelism for alter tablegroup task
     */
    public static int getTableGroupTaskParallelism(ExecutionContext ec) {
        return getScaleoutTaskParallelismImpl(ec, ConnectionParams.TABLEGROUP_TASK_PARALLELISM);
    }

    /**
     * Parallelism for scaleout task
     */
    public static int getScaleoutTaskParallelism(ExecutionContext ec) {
        return getScaleoutTaskParallelismImpl(ec, ConnectionParams.SCALEOUT_TASK_PARALLELISM);
    }

    /**
     * Min(NumCpuCores, Max(4, NumStorageNodes * 2))
     */
    private static int getScaleoutTaskParallelismImpl(ExecutionContext ec, LongConfigParam param) {
        final int minParallelism = 4;
        long parallelism = ec.getParamManager().getLong(param);
        if (parallelism > 0) {
            return (int) parallelism;
        }
        int numCpuCores = ThreadCpuStatUtil.NUM_CORES;
        numCpuCores = Math.max(numCpuCores, 1);
        int numStorageNodes = (int) StorageHaManager.getInstance().getStorageHaCtxCache().values().stream()
            .filter(StorageInstHaContext::isDNMaster)
            .filter(StorageInstHaContext::isAllReplicaReady)
            .count();
        numStorageNodes = Math.max(1, numStorageNodes);
        return Math.min(numCpuCores, Math.max(minParallelism, numStorageNodes * 2));
    }
}
