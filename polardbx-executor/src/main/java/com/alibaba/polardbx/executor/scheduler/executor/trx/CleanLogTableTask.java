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

package com.alibaba.polardbx.executor.scheduler.executor.trx;

import com.alibaba.polardbx.common.IdGenerator;
import com.alibaba.polardbx.common.async.AsyncTask;
import com.alibaba.polardbx.common.constants.SystemTables;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.AsyncUtils;
import com.alibaba.polardbx.common.utils.LockUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.spi.ITopologyExecutor;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.ha.impl.StorageInstHaContext;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.gms.topology.SystemDbHelper.DEFAULT_DB_NAME;

/**
 * @author yaozhlili
 */
public class CleanLogTableTask {
    private static final Logger logger = LoggerFactory.getLogger(CleanLogTableTask.class);

    private static final String GLOBAL_ASYNC_COMMIT_TX_LOG_TABLE = SystemTables.POLARDBX_ASYNC_COMMIT_TX_LOG_TABLE;
    private static final String GLOBAL_ASYNC_COMMIT_TX_LOG_DATABASE = "mysql";
    private static final String GLOBAL_TX_TABLE_MAX_PARTITION = "p_unlimited";

    private static final String GLOBAL_TX_TABLE_GET_PARTITIONS_V2 =
        "SELECT `PARTITION_NAME`, `PARTITION_DESCRIPTION`, `TABLE_ROWS` FROM  INFORMATION_SCHEMA.PARTITIONS\n"
            + "WHERE TABLE_NAME = '" + GLOBAL_ASYNC_COMMIT_TX_LOG_TABLE + "'\n"
            + "AND TABLE_SCHEMA = '" + GLOBAL_ASYNC_COMMIT_TX_LOG_DATABASE + "'";

    private static final String ALTER_GLOBAL_TX_TABLE_ADD_PARTITION_V2 =
        "ALTER TABLE `" + GLOBAL_ASYNC_COMMIT_TX_LOG_DATABASE + "`.`" + GLOBAL_ASYNC_COMMIT_TX_LOG_TABLE + "` \n"
            + "REORGANIZE PARTITION `" + GLOBAL_TX_TABLE_MAX_PARTITION + "` INTO \n"
            + "(PARTITION `%s` VALUES LESS THAN (%d), PARTITION `" + GLOBAL_TX_TABLE_MAX_PARTITION
            + "` VALUES LESS THAN MAXVALUE)";

    private static final String ALTER_GLOBAL_TX_TABLE_DROP_PARTITION_PREFIX_V2 =
        "ALTER TABLE `" + GLOBAL_ASYNC_COMMIT_TX_LOG_DATABASE + "`.`" + GLOBAL_ASYNC_COMMIT_TX_LOG_TABLE + "` \n"
            + "DROP PARTITION ";

    /**
     * Purge trans log created before {@param beforeSeconds}, and split the last partition into
     * [current_max, {@param nextSeconds}) and [{@param nextSeconds}, unlimited) if necessary.
     *
     * @return approximate number of purged trans log
     */
    public static int run(int beforeSeconds, int nextSeconds) {
        final long nowTimeMillis = System.currentTimeMillis();
        final long beforeTimeMillis = nowTimeMillis - beforeSeconds * 1000L;
        final long beforeTxid = IdGenerator.assembleId(beforeTimeMillis, 0, 0);
        final long nextTimeMillis = nowTimeMillis + nextSeconds * 1000L;
        final long nextTxid = IdGenerator.assembleId(nextTimeMillis, 0, 0);

        Set<String> dnIds =
            StorageHaManager.getInstance().getMasterStorageList().stream().filter(s -> !s.isMetaDb())
                .map(StorageInstHaContext::getStorageInstId).collect(Collectors.toSet());
        List<Future> futures = new ArrayList<>();
        ITopologyExecutor executor = ExecutorContext.getContext(DEFAULT_DB_NAME).getTopologyExecutor();
        AtomicInteger purgeCount = new AtomicInteger(0);
        for (String dnId : dnIds) {
            futures.add(executor.getExecutorService().submit(DEFAULT_DB_NAME, null, AsyncTask.build(() -> {
                try (Connection conn = DbTopologyManager.getConnectionForStorage(dnId)) {
                    LockUtil.wrapWithLockWaitTimeout(conn, 60,
                        stmt -> purgeCount.addAndGet(rotateV2(stmt, beforeTxid, nextTxid)));
                } catch (SQLException e) {
                    logger.error("Log clean task failed.", e);
                }
            })));
        }
        AsyncUtils.waitAll(futures);
        return purgeCount.get();
    }

    private static int rotateV2(Statement stmt, long beforeTxid, long nextTxid) {
        try {
            int dropped = 0;
            ArrayList<String> partitionsWillDrop = new ArrayList<>();
            long txidUpperBound = Long.MIN_VALUE;
            try (ResultSet rs = stmt.executeQuery(GLOBAL_TX_TABLE_GET_PARTITIONS_V2)) {
                while (rs.next()) {
                    final String partitionName = rs.getString(1);
                    if (partitionName == null) {
                        throw new TddlRuntimeException(ErrorCode.ERR_TRANS_LOG,
                            "Rotate global tx log on non-partitioned table");
                    }
                    final String partitionDescText = rs.getString(2);
                    if ("MAXVALUE".equalsIgnoreCase(partitionDescText)) {
                        continue;
                    }
                    try {
                        long maxTxidInPartition = Long.parseLong(partitionDescText);
                        if (maxTxidInPartition < beforeTxid) {
                            final long tableRows = rs.getLong(3);
                            dropped += tableRows;
                            partitionsWillDrop.add("`" + partitionName + "`");
                        }
                        txidUpperBound = Math.max(txidUpperBound, maxTxidInPartition);
                    } catch (NumberFormatException e) {
                        throw new TddlRuntimeException(ErrorCode.ERR_TRANS_LOG,
                            "Invalid partition description for partition " + partitionName);
                    }
                }
            }
            if (nextTxid > txidUpperBound) {
                logger.info("Creating new partition" + "p_" + nextTxid + " on async commit tx log");
                stmt.executeUpdate(String.format(ALTER_GLOBAL_TX_TABLE_ADD_PARTITION_V2, "p_" + nextTxid, nextTxid));
            }
            if (!partitionsWillDrop.isEmpty()) {
                String dropSql = ALTER_GLOBAL_TX_TABLE_DROP_PARTITION_PREFIX_V2 + String.join(",", partitionsWillDrop);
                logger.info("Purging async commit tx log with ddl " + dropSql.replace("\n", " "));
                stmt.executeUpdate(dropSql);
            }
            return dropped;
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_TRANS_LOG, e,
                "Rotate global transaction log with " + beforeTxid + " failed");
        }
    }
}
