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

package com.alibaba.polardbx.transaction.tso;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.utils.ITimestampOracle;
import com.alibaba.polardbx.rpc.pool.XConnection;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @version 1.0
 */
public class ClusterTimestampOracle extends AbstractLifecycle implements ITimestampOracle {

    private static final Logger logger = LoggerFactory.getLogger(ClusterTimestampOracle.class);

    private long timeout = 10000;

    private static final int BitReserved = 6;

    @Override
    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    class TsoFuture {
        private final AtomicLong tso = new AtomicLong(0);
        private final AtomicReference<Exception> exception = new AtomicReference<>(null);
        private final FutureTask<Long> futureTask = new FutureTask<>(tso::get);

        public void setTso(long tso) {
            this.tso.set(tso);
            futureTask.run();
        }

        public void setException(Exception exception) {
            this.exception.set(exception);
            futureTask.run();
        }

        public long waitTso() throws Exception {
            try {
                final long tso = futureTask.get(timeout, TimeUnit.MILLISECONDS);
                if (exception.get() != null) {
                    throw exception.get();
                }
                return tso;
            } catch (TimeoutException e) {
                throw new TimeoutException("Fetch TSO timeout.");
            }
        }
    }

    private static final ArrayList<TsoFuture> taskQueue = new ArrayList<>();

    static private void fetchTsoTask() {
        ArrayList<TsoFuture> copyed = null;
        synchronized (taskQueue) {
            while (taskQueue.isEmpty()) {
                try {
                    taskQueue.wait();
                } catch (InterruptedException ignore) {
                }
            }
            copyed = (ArrayList<TsoFuture>) taskQueue.clone();
            taskQueue.clear();
        }

        final int fetchTimeout = 10000; // 10s
        long tsoBase = 0;
        Exception exception = null;

        // Get TSO.
        final long startTime = System.currentTimeMillis();
        while (true) {
            // Add new waited.
            synchronized (taskQueue) {
                if (!taskQueue.isEmpty()) {
                    copyed.addAll(taskQueue);
                    taskQueue.clear();
                }
            }

            try (Connection metaDbConn = MetaDbUtil.getConnection()) {
                if (metaDbConn.isWrapperFor(XConnection.class)) {
                    final XConnection xConnection = metaDbConn.unwrap(XConnection.class);
                    final int oldTimeout = xConnection.getNetworkTimeout();
                    try {
                        xConnection.setNetworkTimeout(null, fetchTimeout);
                        tsoBase = xConnection.getTSO(copyed.size());
                        // Success.
                        break;
                    } finally {
                        xConnection.setNetworkTimeout(null, oldTimeout);
                    }
                } else {
                    // JDBC.
                    try (Statement statement = metaDbConn.createStatement()) {
                        statement.setQueryTimeout(fetchTimeout / 1000);
                        try (ResultSet rs = statement.executeQuery(
                            "call dbms_tso.get_timestamp('mysql', 'gts_base'," + copyed.size() + ")")) {
                            if (rs.next()) {
                                tsoBase = rs.getLong(1);
                                // Success.
                                break;
                            }
                            throw new TddlRuntimeException(ErrorCode.ERR_GMS_UNEXPECTED,
                                "Cannot get TSO with unexpected column.");
                        }
                    }
                }
            } catch (Exception e) {
                // Retry if HA occurs and not timeout.
                if (e instanceof SQLException || e.getMessage().contains("Failed to get TSO") || e.getMessage()
                    .contains("channel inactive")) {
                    if (System.currentTimeMillis() - startTime < fetchTimeout) {
                        continue;
                    }
                }
                exception = e;
                break;
            }
        }

        // Ok now report success or error.
        if (exception != null) {
            for (TsoFuture future : copyed) {
                future.setException(exception);
            }
        } else {
            long tso = tsoBase;
            for (TsoFuture future : copyed) {
                future.setTso(tso += (1 << BitReserved));
            }
        }
    }

    static {
        final Runnable tsoAsyncTask = () -> {
            while (true) {
                try {
                    fetchTsoTask();
                } catch (Throwable t) {
                    logger.error(t);
                }
            }
        };

        (new Thread(tsoAsyncTask, "TsoFetcher")).start();
    }

    private long nextTimestampGrouping() {
        final TsoFuture future = new TsoFuture();
        synchronized (taskQueue) {
            taskQueue.add(future);
            taskQueue.notify();
        }
        try {
            return future.waitTso();
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public long nextTimestamp() {
        return nextTimestampGrouping();
    }

}
