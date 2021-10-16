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

package com.alibaba.polardbx.transaction.async;

import com.alibaba.polardbx.common.utils.AsyncUtils;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.transaction.TransactionExecutor;
import com.alibaba.polardbx.optimizer.utils.ITimestampOracle;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

/**
 * Task to send a timestamp to storage nodes in order to keep their latest timestamp up-to-date.
 * Only works on PolarDB-X cluster.
 *
 * @author Eric Fu
 */
public class TsoHeartbeatTask implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(TsoHeartbeatTask.class);

    private static final String HEARTBEAT_QUERY = "SET GLOBAL innodb_heartbeat_seq = ?";

    private final AsyncTaskQueue asyncQueue;
    private final TransactionExecutor executor;
    private final ITimestampOracle tso;

    public TsoHeartbeatTask(AsyncTaskQueue asyncQueue, TransactionExecutor executor,
                            ITimestampOracle tso) {
        this.asyncQueue = asyncQueue;
        this.executor = executor;
        this.tso = tso;
    }

    @Override
    public void run() {
        boolean hasLeadership = ExecUtils.hasLeadership(null);
        if (!hasLeadership) {
            return;
        }

        long timestamp = tso.nextTimestamp();

        List<Future> futures = new ArrayList<>();
        for (String group : executor.getGroupList()) {
            DataSource dataSource = executor.getGroupExecutor(group).getDataSource();
            // Send heartbeat to each group simultaneously
            futures.add(asyncQueue.submit(() -> {
                doHeartbeat(dataSource, timestamp);
            }));
        }

        // Also send heartbeat to MetaDB
        DataSource dataSource = MetaDbDataSource.getInstance().getDataSource();
        futures.add(asyncQueue.submit(() -> {
            doHeartbeat(dataSource, timestamp);
        }));

        AsyncUtils.waitAll(futures);
    }

    private void doHeartbeat(DataSource dataSource, long timestamp) {
        try (Connection conn = dataSource.getConnection();
            PreparedStatement ps = conn.prepareStatement(HEARTBEAT_QUERY)) {
            ps.setLong(1, timestamp);
            ps.executeUpdate();
        } catch (SQLException e) {
            logger.error("Failed to send timestamp heartbeat", e);
        }
    }
}
