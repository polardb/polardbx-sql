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

import com.alibaba.polardbx.common.DefaultSchema;
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
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.executor.ddl.workqueue.BackFillThreadPool;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.ha.impl.StorageInstHaContext;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.topology.DbGroupInfoAccessor;
import com.alibaba.polardbx.gms.topology.DbGroupInfoRecord;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.config.table.ScaleOutPlanUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.dialect.DbType;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ReplaceTableNameWithQuestionMarkVisitor;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlMoveDatabase;
import org.apache.calcite.sql.SqlNode;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

public class ScaleOutUtils {

    public static final int RETRY_COUNT = 3;
    public static final long[] RETRY_WAIT = new long[RETRY_COUNT];

    static {
        IntStream.range(0, RETRY_COUNT).forEach(i -> RETRY_WAIT[i] = Math.round(Math.pow(2, i)));
    }

    public static final String DEFAULT_PARAMETER_METHOD = "setObject1";

    private static final String DEFAULT_CATALOG = "def";
    private static final String CHECKSUM_TABLE = "checksum table ";
    private static final String DROP_TABLE_IF_EXISTS = "drop table if exists ";
    // 不能和逻辑表的backfill共用一个线程池，防止死锁，因为backfill是其子任务
    private static final BackFillThreadPool LOGICAL_TABLE_PARALLEL_INSTANCE_POOL = new BackFillThreadPool();

    private static SqlNode getSqlTemplate(String primaryTableDefinition, ExecutionContext ec) {
        final SqlCreateTable primaryTableNode = (SqlCreateTable) new FastsqlParser()
            .parse(primaryTableDefinition)
            .get(0);
        ReplaceTableNameWithQuestionMarkVisitor visitor =
            new ReplaceTableNameWithQuestionMarkVisitor(DefaultSchema.getSchemaName(), ec);
        return primaryTableNode.accept(visitor);
    }

    private static Pair<BytesSql, Map<Integer, ParameterContext>> buildSqlAndParam(List<String> tableNames,
                                                                                   SqlNode sqlTemplate,
                                                                                   Map<Integer, ParameterContext> param,
                                                                                   List<Integer> paramIndex) {
        Preconditions.checkArgument(GeneralUtil.isNotEmpty(tableNames));
        BytesSql sql = RelUtils.toNativeBytesSql(sqlTemplate, DbType.MYSQL);
        Preconditions.checkArgument(GeneralUtil.isNotEmpty(tableNames));
        Map<Integer, ParameterContext> params = PlannerUtils.buildParam(tableNames, param, paramIndex);
        return new Pair<>(sql, params);
    }

    public static boolean checkCheckSum(String schemaName, String sourceGroup, String targetGroup, String logicTable) {
        List<String> physicalTables = ScaleOutPlanUtil.getPhysicalTables(sourceGroup, schemaName, logicTable);
        for (String physicalTable : physicalTables) {
            BigDecimal sourceTableCheckSum = getCheckSumResult(schemaName, sourceGroup, physicalTable);
            BigDecimal targetTableCheckSum = getCheckSumResult(schemaName, targetGroup, physicalTable);
            if (BigDecimal.valueOf(Long.MIN_VALUE).equals(sourceTableCheckSum)
                || BigDecimal.valueOf(Long.MIN_VALUE).equals(targetTableCheckSum)
                || (sourceTableCheckSum != null && !sourceTableCheckSum.equals(targetTableCheckSum))
                || (targetTableCheckSum != null && !targetTableCheckSum.equals(sourceTableCheckSum))) {
                return false;
            }
        }
        return true;
    }

    private static BigDecimal getCheckSumResult(String schemaName, String groupkey, String physicalTable) {
        TGroupDataSource dataSource = getDataSource(schemaName, groupkey);
        if (dataSource != null) {
            try (Connection conn = dataSource.getConnection(MasterSlave.MASTER_ONLY);
                PreparedStatement ps = conn.prepareStatement(CHECKSUM_TABLE + physicalTable)) {
                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    return rs.getBigDecimal(2);
                }
            } catch (SQLException e) {
            }
        }
        return BigDecimal.valueOf(Long.MIN_VALUE);
    }

    private static TGroupDataSource getDataSource(String SchemaName, String groupName) {
        TopologyHandler topology = ExecutorContext.getContext(SchemaName).getTopologyHandler();
        Object dataSource = topology.get(groupName).getDataSource();

        if (dataSource != null && dataSource instanceof TGroupDataSource) {
            return (TGroupDataSource) dataSource;
        }
        return null;
    }

    public static void setGroupTypeByDbAndGroup(String schemaName, String groupName, int groupType) {
        boolean isSuccess = false;
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {
            try {
                metaDbConn.setAutoCommit(false);
                DbGroupInfoAccessor dbGroupInfoAccessor = new DbGroupInfoAccessor();
                dbGroupInfoAccessor.setConnection(metaDbConn);

                dbGroupInfoAccessor
                    .updateGroupTypeByDbAndGroup(schemaName, groupName,
                        groupType);
                metaDbConn.commit();
                isSuccess = true;
            } finally {
                if (!isSuccess) {
                    metaDbConn.rollback();
                }
            }
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
    }

    public static BackFillThreadPool getLogicalTableParallelInstancePool() {
        return LOGICAL_TABLE_PARALLEL_INSTANCE_POOL;
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
