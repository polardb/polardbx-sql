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
import com.alibaba.polardbx.common.utils.GeneralUtil;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * <pre>
 *
 * 封装了dataSource的lazyInit逻辑的DataSourceWrapper,
 * 当真正需要使用数据源时，就会触发真正的数据源初始化逻辑，
 * 主要用于主备切换场景
 *
 *
 * 使用场景：
 * 在主备切换（特别是过程中是有更新dbKey的情况），会造成新库的重新初始化，
 *
 * 库在初始化过程中可能会出错，为避免因为数据源的初始化出错导致主备切换的新的主备信息无法生效，
 * 将所有在初始化过程中出错的新的dbKey改为lazyInit模式，保留在应用真正查询数据库再重新进行初始化
 * </pre>
 *
 * @author chenghui.lch 2017年1月20日 下午2:52:29
 * @since 5.0.0
 */
public class DataSourceLazyInitWrapper extends DataSourceWrapper {

    /**
     * 实际的数据源获取器，lazyInit的关键对象
     */
    protected DataSourceFetcher fetcher;

    public DataSourceLazyInitWrapper(String dataSourceKey, String weightStr, DataSourceFetcher fetcher,
                                     int dataSourceIndex) {
        super(dataSourceKey, weightStr, null, dataSourceIndex);
        this.fetcher = fetcher;
        this.isInited = false;
    }

    @Override
    protected void doInit() {
        if (wrappedDataSource == null) {
            wrappedDataSource = fetcher.getDataSource(this.dbKey);
            if (wrappedDataSource != null) {
                if (weight.a > 0) {
                    wrappedDataSource.setDsModeInGroupKey(TddlConstants.DS_MODE_AP);
                } else {
                    wrappedDataSource.setDsModeInGroupKey(null);
                }
            }
        }
    }

    protected TAtomDataSource getDataSourceInner() {

        try {
            if (wrappedDataSource == null) {
                if (!isInited) {
                    init();
                }
            }
            return wrappedDataSource;
        } catch (Throwable e) {

            // 物理数据源初始化失败， 则直接报错
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    public TAtomDataSource getWrappedDataSource() {
        return getDataSourceInner();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return getDataSourceInner().getConnection();
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return getDataSourceInner().getConnection(username, password);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return getDataSourceInner().getLogWriter();
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return getDataSourceInner().getLoginTimeout();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        getDataSourceInner().setLogWriter(out);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        getDataSourceInner().setLoginTimeout(seconds);
    }

}
