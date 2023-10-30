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

package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.delegate.MetaDbAccessorWrapper;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public abstract class GsiStatisticsAccessorDelegate<T> extends MetaDbAccessorWrapper<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexesAccessor.class);

    protected final IndexesAccessor indexesAccessor;

    protected Connection connection;

    public GsiStatisticsAccessorDelegate() {
        this.indexesAccessor = new IndexesAccessor();
    }

    @Override
    protected void open(Connection metaDbConn) {
        this.indexesAccessor.setConnection(metaDbConn);
        this.connection = metaDbConn;
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
                MetaDbUtil.rollback(metaDbConn, new RuntimeException(t), LOGGER, "remove table meta");
                onException(metaDbConn, t);
                throw t;
            } finally {
                MetaDbUtil.endTransaction(metaDbConn, LOGGER);
                close();
            }
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        }
    }

    protected void onException(Connection metaDbConn, Throwable throwable) {
    }

    @Override
    protected void close() {
        this.indexesAccessor.setConnection(null);
    }

    public Connection getConnection() {
        return this.connection;
    }

}
