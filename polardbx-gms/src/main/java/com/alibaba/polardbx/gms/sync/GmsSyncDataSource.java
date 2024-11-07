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

import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Properties;

public class GmsSyncDataSource extends AbstractLifecycle {

    private static final MessageFormat MYSQL_URL_FORMAT = new MessageFormat("jdbc:mysql://{0}:{1}/{2}?useSSL=false");

    private static final String SYNC_PASSWORD = "none";
    private static final String SYNC_DATABASE = "sync";

    private final String jdbcUrl;
    private final Properties connInfo;

    public GmsSyncDataSource(String instId, String host, String port) {
        this.jdbcUrl = MYSQL_URL_FORMAT.format(new String[] {host, port, SYNC_DATABASE});
        this.connInfo = new Properties();
        this.connInfo.setProperty("user", instId);
        this.connInfo.setProperty("password", SYNC_PASSWORD);
    }

    @Override
    protected void doInit() {
        super.doInit();
    }

    @Override
    protected void doDestroy() {
        super.doDestroy();
    }

    public Connection getConnection() throws SQLException {
        return getDirectConnection();
    }

    private Connection getDirectConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl, connInfo);
    }

}
