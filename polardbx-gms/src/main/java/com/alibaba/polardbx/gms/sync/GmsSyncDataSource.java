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

package com.alibaba.polardbx.gms.sync;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.thread.ExtendedScheduledThreadPoolExecutor;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import org.apache.commons.lang.StringUtils;

import javax.sql.CommonDataSource;
import javax.sql.DataSource;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.text.MessageFormat;
import java.util.concurrent.ScheduledExecutorService;
import java.util.logging.Logger;

public class GmsSyncDataSource extends AbstractLifecycle implements DataSource {

    private static MessageFormat MYSQL_URL_FORMAT = new MessageFormat("jdbc:mysql://{0}:{1}/{2}");

    private static ScheduledExecutorService createScheduler = createScheduler("GMS-Druid-CreateScheduler-", 30);
    private static ScheduledExecutorService destroyScheduler = createScheduler("GMS-Druid-DestroyScheduler-", 30);

    private DruidDataSource druidDataSource;

    private final String instId;
    private final String host;
    private final String port;

    public GmsSyncDataSource(String instId, String host, String port) {
        this.instId = instId;
        this.host = host;
        this.port = port;
    }

    @Override
    protected void doInit() {
        super.doInit();

        DruidDataSource druidDataSource = null;
        boolean hasInitErr = false;
        try {
            druidDataSource = buildDataSource();
            druidDataSource.init();
            this.druidDataSource = druidDataSource;
        } catch (SQLException e) {
            hasInitErr = true;
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC,
                "Failed to initialize Druid data source for node sync");
        } finally {
            if (hasInitErr) {
                if (druidDataSource != null) {
                    druidDataSource.close();
                }
            }
        }
    }

    private DruidDataSource buildDataSource() {
        DruidDataSource druidDataSource = new DruidDataSource();

        druidDataSource.setUsername(instId);
        druidDataSource.setPassword("none");
        druidDataSource.setUrl(MYSQL_URL_FORMAT.format(new String[] {host, port, "sync"}));
        druidDataSource.setName("node_" + host + "_" + port);

        // Acquire sync-lock while connection is closing
        druidDataSource.setAsyncCloseConnectionEnable(true);

        druidDataSource.setCreateScheduler(createScheduler);
        druidDataSource.setDestroyScheduler(destroyScheduler);

        druidDataSource.setTestOnBorrow(false);
        druidDataSource.setTestWhileIdle(true);
        druidDataSource.setLogDifferentThread(false);

        druidDataSource.setFailFast(true);
        druidDataSource.setNotFullTimeoutRetryCount(2);
        druidDataSource.setDriverClassName(MetaDbDataSource.DEFAULT_DRIVER_CLASS);
        druidDataSource.setValidationQuery(MetaDbDataSource.DEFAULT_VALIDATION_QUERY);

        druidDataSource.setOnFatalErrorMaxActive(8);

        druidDataSource.setMinIdle(1);
        druidDataSource.setMaxActive(30);

        druidDataSource.setMinEvictableIdleTimeMillis(60 * 1000);
        druidDataSource.setKeepAliveBetweenTimeMillis(60 * 1000 / 30);
        druidDataSource.setTimeBetweenConnectErrorMillis(3 * 1000);
        druidDataSource.setMaxWait(3 * 1000);

        druidDataSource.setDriverClassLoader(druidDataSource.getClass().getClassLoader());
        druidDataSource.setUseUnfairLock(true);

        return druidDataSource;
    }

    @Override
    protected void doDestroy() {
        super.doDestroy();
        if (druidDataSource != null) {
            druidDataSource.close();
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        return druidDataSource.getConnection();
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return druidDataSource.getConnection(username, password);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (isWrapperFor(iface)) {
            return (T) this;
        } else {
            throw new SQLException("Not a wrapper for " + iface);
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return GmsSyncDataSource.class.isAssignableFrom(iface);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return druidDataSource.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        druidDataSource.setLogWriter(out);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        druidDataSource.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return druidDataSource.getLoginTimeout();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        try {
            Method getParentLoggerMethod = CommonDataSource.class.getDeclaredMethod("getParentLogger", new Class<?>[0]);
            return (Logger) getParentLoggerMethod.invoke(druidDataSource, new Object[0]);
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

    private static ScheduledExecutorService createScheduler(String name, int poolSize) {
        String systemPoolSize = System.getProperty("tddl.scheduler.poolSize");
        if (StringUtils.isNotEmpty(systemPoolSize)) {
            poolSize = Integer.valueOf(systemPoolSize);
        }
        return new ExtendedScheduledThreadPoolExecutor(poolSize, new NamedThreadFactory(name, true));
    }

}
