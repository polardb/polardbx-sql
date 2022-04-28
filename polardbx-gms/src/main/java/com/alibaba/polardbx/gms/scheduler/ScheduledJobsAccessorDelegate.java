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

package com.alibaba.polardbx.gms.scheduler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.delegate.MetaDbAccessorWrapper;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.Connection;
import java.sql.SQLException;

public abstract class ScheduledJobsAccessorDelegate<T> extends MetaDbAccessorWrapper<T> {

    protected static final Logger LOGGER = LoggerFactory.getLogger(ScheduledJobsAccessorDelegate.class);

    protected ScheduledJobsAccessor scheduledJobsAccessor;
    protected FiredScheduledJobsAccessor firedScheduledJobsAccessor;
    protected Connection connection;

    public ScheduledJobsAccessorDelegate() {
        this.scheduledJobsAccessor = new ScheduledJobsAccessor();
        this.firedScheduledJobsAccessor = new FiredScheduledJobsAccessor();
    }

    @Override
    protected void open(Connection metaDbConn) {
        this.connection = metaDbConn;
        this.scheduledJobsAccessor.setConnection(metaDbConn);
        this.firedScheduledJobsAccessor.setConnection(metaDbConn);
    }

    @Override
    public T execute() {
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            try {
                open(metaDbConn);
                MetaDbUtil.beginTransaction(metaDbConn);

                T r = invoke();

                MetaDbUtil.commit(metaDbConn);
                return r;
            } catch (Throwable t) {
                MetaDbUtil.rollback(metaDbConn, new RuntimeException(t), LOGGER, "process scheduled jobs");
                throw t;
            } finally {
                MetaDbUtil.endTransaction(metaDbConn, LOGGER);
                close();
            }
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        }
    }

    @Override
    protected void close() {
        this.scheduledJobsAccessor.setConnection(null);
        this.firedScheduledJobsAccessor.setConnection(null);
        this.connection = null;
    }

    public Connection getConnection() {
        return this.connection;
    }
}