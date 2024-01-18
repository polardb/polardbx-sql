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

package com.alibaba.polardbx.group.jdbc;

import com.alibaba.polardbx.atom.TAtomDataSource;
import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.group.config.Weight;

import javax.sql.CommonDataSource;
import javax.sql.DataSource;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

/**
 * <pre>
 * 一个线程安全的DataSource包装类 DataSource包装类，
 * 因为一个GroupDataSource由多个AtomDataSource组成，
 * 且每个AtomDataSource都有对应的读写权重等信息，
 * 所以将每一个AtomDataSource封装起来。
 *
 *
 * ---add by mazhidan.pt
 * </pre>
 *
 * @author linxuan refactor as immutable class; dataSourceIndex extends
 */
public class DataSourceWrapper extends AbstractLifecycle implements DataSource {

    protected String storageId;
    protected final String dbKey;             // 这个DataSource对应的dbKey
    protected final String weightStr;         // 权重信息字符串
    protected final Weight weight;            // 权重信息
    protected volatile TAtomDataSource wrappedDataSource; // 被封装的目标DataSource
    protected final int dataSourceIndex;   // DataSourceIndex是指这个DataSource在Group中的位置

    public DataSourceWrapper(String storageId, String dbKey, String weightStr, TAtomDataSource wrappedDataSource,
                             int dataSourceIndex) {
        this.dbKey = dbKey;
        this.weight = new Weight(weightStr);
        this.weightStr = weightStr;
        this.wrappedDataSource = wrappedDataSource;

        if (wrappedDataSource != null) {
            if (weight.a > 0) {
                wrappedDataSource.setDsModeInGroupKey(TddlConstants.DS_MODE_AP);
            } else {
                wrappedDataSource.setDsModeInGroupKey(null);
            }
        }

        this.dataSourceIndex = dataSourceIndex;
        this.isInited = true;
        this.storageId = storageId;
    }

    /**
     * 是否有读权重。r0则放回false
     */
    public boolean hasReadWeight() {
        return weight.r != 0;
    }

    /**
     * 是否有写权重。w0则放回false
     */
    public boolean hasWriteWeight() {
        return weight.w != 0;
    }

    @Override
    public String toString() {
        return new StringBuilder("DataSourceWrapper{dataSourceKey=").append(dbKey)
            .append(", dataSourceIndex=")
            .append(dataSourceIndex)
            .append(",weight=")
            .append(weight)
            .append("}")
            .toString();
    }

    public String getDataSourceKey() {
        return dbKey;
    }

    public String getWeightStr() {
        return weightStr;
    }

    public Weight getWeight() {
        return weight;
    }

    public TAtomDataSource getWrappedDataSource() {
        return wrappedDataSource;
    }

    // 以下是javax.sql.DataSource的API实现
    // //////////////////////////////////////////////////////////////////////////
    @Override
    public Connection getConnection() throws SQLException {
        return wrappedDataSource.getConnection();
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return wrappedDataSource.getConnection(username, password);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return wrappedDataSource.getLogWriter();
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return wrappedDataSource.getLoginTimeout();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        wrappedDataSource.setLogWriter(out);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        wrappedDataSource.setLoginTimeout(seconds);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(this.getClass());
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            return (T) this;
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        try {
            Method getParentLoggerMethod = CommonDataSource.class.getDeclaredMethod("getParentLogger");
            return (java.util.logging.Logger) getParentLoggerMethod.invoke(this.wrappedDataSource, new Object[0]);
        } catch (NoSuchMethodException e) {
            throw new SQLFeatureNotSupportedException(e);
        } catch (InvocationTargetException e2) {
            throw new SQLFeatureNotSupportedException(e2);
        } catch (IllegalArgumentException e2) {
            throw new SQLFeatureNotSupportedException(e2);
        } catch (IllegalAccessException e2) {
            throw new SQLFeatureNotSupportedException(e2);
        }
    }

    public int getDataSourceIndex() {
        return dataSourceIndex;
    }

    public String getStorageId() {
        return storageId;
    }
}
