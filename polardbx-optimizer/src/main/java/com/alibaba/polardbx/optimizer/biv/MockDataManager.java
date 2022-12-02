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

package com.alibaba.polardbx.optimizer.biv;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.parse.SqlTypeParser;
import com.alibaba.polardbx.optimizer.parse.SqlTypeUtils;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import io.grpc.netty.shaded.io.netty.util.internal.StringUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class MockDataManager {

    private static DruidDataSource mockDs;

    public static final String MOCK_DB_URL = "jdbc:mysql://case.mysql.com:";
    public static final String MOCK_DB_USERNAME = "MOCK_DB_USERNAME";
    public static final String MOCK_DB_PORT = "MOCK_DB_PORT";
    public static final String MOCK_DB_PASSWORD = "MOCK_DB_PASSWORD";

    private static final LoadingCache<String, MockCacheData> cacheData = CacheBuilder.newBuilder()
        .expireAfterAccess(10000, TimeUnit.SECONDS)
        .maximumSize(1000)
        .build(new CacheLoader<String, MockCacheData>() {
            @Override
            public MockCacheData load(String mockDataKey) throws Exception {
                return playBack(mockDataKey);
            }
        });

    public static final LoadingCache<String, Integer> traceIds = CacheBuilder.newBuilder()
        .maximumSize(1000)
        .build(new CacheLoader<String, Integer>() {
            @Override
            public Integer load(String key) throws Exception {
                return 0;
            }
        });

    private static final String CREATE_TEST_TABLE = "CREATE TABLE IF NOT EXISTS `test` (\n"
        + "  `id` int(11) NOT NULL AUTO_INCREMENT,\n"
        + "  `name` varchar(10) DEFAULT NULL,\n"
        + "  PRIMARY KEY (`id`)\n"
        + ") ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8";

    private static MockCacheData playBack(String mockDataKey) throws SQLException {
        try (Connection c = getConnection()) {
            prepareDB(c);
            MockCacheData mockCacheData = mockData(c, mockDataKey);
//            cleanDB(c);
            return mockCacheData;
        }
    }

    /**
     * build connection for mock mode
     */
    private static Connection getConnection() throws SQLException {
        if (MetaDbDataSource.getInstance() == null) {
            return tryAssignDBConn();
        }
        Connection c = MetaDbDataSource.getInstance().getConnection();
        if (c == null) {
            return tryAssignDBConn();
        } else {
            return c;
        }
    }

    private static Connection tryAssignDBConn() throws SQLException {
        if (mockDs == null) {
            // try init mock ds
            mockDs = new DruidDataSource();
            String port = System.getenv(MOCK_DB_PORT);
            mockDs.setUrl(StringUtil.isNullOrEmpty(port) ? MOCK_DB_URL + "3306" : MOCK_DB_URL + port);
            mockDs.setUsername(System.getenv(MOCK_DB_USERNAME));
            mockDs.setPassword(System.getenv(MOCK_DB_PASSWORD));
        }
        return mockDs.getConnection();
    }

    private static void cleanDB(Connection c) throws SQLException {
        Statement statement = null;
        try {
            // prepare db
            statement = c.createStatement();
            String dbName = "__MOCK_DB__";
            statement.execute("DROP DATABASE IF EXISTS " + dbName);
        } catch (Exception e) {
            throw e;
        } finally {
            if (statement != null) {
                statement.close();
            }
        }
    }

    private static MockCacheData mockData(Connection c, String sql) throws SQLException {
        SqlType sqlType = SqlTypeParser.typeOf(sql);
        if (!SqlTypeUtils.isSelectSqlType(sqlType)
            && sqlType != SqlType.SHOW
            && !SqlTypeUtils.isShowSqlType(sqlType)) {
            int dmlNum = c.createStatement().executeUpdate(sql);
            return new MockCacheData(sql, dmlNum);
        }

        if (sqlType == null) {// like XA
            int dmlNum = c.createStatement().executeUpdate(sql);
            return new MockCacheData(sql, dmlNum);
        }

        ResultSet resultSet = c.createStatement().executeQuery(sql);

        resultSet.getMetaData();
        List<List<Object>> datasList = Lists.newArrayList();
        int columnCount = resultSet.getMetaData().getColumnCount();

        while (columnCount != 0 && resultSet.next()) {
            List<Object> data = Lists.newArrayList();
            for (int i = 1; i <= columnCount; i++) {
                data.add(resultSet.getObject(i));
            }
            datasList.add(data);
        }
        MockCacheData mockCacheData = new MockCacheData(sql, resultSet.getMetaData(), datasList);
        return mockCacheData;
    }

    private static void prepareDB(Connection c) throws SQLException {
        try (Statement statement = c.createStatement()) {
            // prepare db
            String dbName = "__MOCK_DB__";
            statement.execute("CREATE DATABASE IF NOT EXISTS " + dbName);
            statement.execute("use " + dbName);
            statement.execute(CREATE_TEST_TABLE);
        } catch (Exception e) {
            throw e;
        }
    }

    public static MockCacheData buildCacheData(String sql, MockConnection c) throws SQLException {

        try {
            return cacheData.get(sql);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof SQLException) {
                throw (SQLException) e.getCause();
            }
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public static boolean isMockEnoughData(String key) {
        if (key.contains("__drds__systable__")) {
            return false;
        }
        Integer value;
        try {
            value = traceIds.get(key);
        } catch (ExecutionException e) {
            value = 0;
        }
        traceIds.put(key, value + 1);

        return value < 5;
    }

}
