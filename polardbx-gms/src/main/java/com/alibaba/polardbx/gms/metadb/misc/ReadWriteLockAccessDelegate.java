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

package com.alibaba.polardbx.gms.metadb.misc;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.gms.metadb.delegate.MetaDbAccessorWrapper;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.Connection;
import java.sql.SQLException;

public abstract class ReadWriteLockAccessDelegate<T> extends MetaDbAccessorWrapper<T> {

    protected final ReadWriteLockAccessor accessor;
    protected Connection connection;

    public ReadWriteLockAccessDelegate() {
        this.accessor = new ReadWriteLockAccessor();
    }

    @Override
    protected void open(Connection metaDbConn) {
        this.accessor.setConnection(metaDbConn);
        this.connection = metaDbConn;
    }

    @Override
    protected void close() {
        this.accessor.setConnection(null);
    }

    public Connection getConnection() {
        return this.connection;
    }

    @Override
    public T execute() {
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            int iso = metaDbConn.getTransactionIsolation();
            try {
                open(metaDbConn);
                metaDbConn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                T r = invoke();
                return r;
            } finally {
                metaDbConn.setTransactionIsolation(iso);
                close();
            }
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e, e.getMessage());
        }
    }
}
