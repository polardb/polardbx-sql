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

package com.alibaba.polardbx.qatest.oss;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.oss.utils.FileStorageTestUtil;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import com.google.common.collect.Queues;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MultiTableExpireTest extends BaseTestCase {
    private static String testDataBase = "db_test_valid";

    private static Engine engine = PropertiesUtil.engine();

    private static final int TABLE_COUNT = 10;

    @BeforeClass
    static public void initTestDatabase() {
        try (Connection conn = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            Statement statement = conn.createStatement();
            statement.execute(String.format("drop database if exists %s ", testDataBase));
            statement.execute(String.format("create database %s mode = 'auto'", testDataBase));
            statement.execute(String.format("use %s", testDataBase));
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Before
    public void test() {
        try (Connection connection = getPolardbxConnection(testDataBase)) {

            for (int tableIndex = 1; tableIndex < TABLE_COUNT; tableIndex++) {
                String innodbTable = "t_order_" + tableIndex;
                String ossTable = "t_order_" + tableIndex + "_oss";

                FileStorageTestUtil.prepareInnoTable(connection, innodbTable, 2000);
                FileStorageTestUtil.prepareTTLTable(connection, ossTable, innodbTable, engine);
            }

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testExpire() throws InterruptedException {
        ExecutorService threadPool = Executors.newCachedThreadPool();
        for (int i = 1; i < TABLE_COUNT; i++) {
            final int tableIndex = i;
            threadPool.submit(() -> {
                String innodbTable = "t_order_" + tableIndex;

                try (Connection connection = getPolardbxConnection(testDataBase)) {
                    // get local partition
                    List<String> localPartitions = new ArrayList<>();
                    ResultSet rs = JdbcUtil.executeQuery(String.format(
                        "select LOCAL_PARTITION_NAME from information_schema.local_partitions where table_schema=\"%s\" and table_name=\"%s\"",
                        testDataBase, innodbTable), connection);
                    while (rs.next()) {
                        localPartitions.add(rs.getString("LOCAL_PARTITION_NAME"));
                    }
                    rs.close();

                    Statement statement = connection.createStatement();
                    for (int j = 0; j < 10; j++) {
                        statement.execute(String.format("ALTER TABLE %s EXPIRE LOCAL PARTITION '%s'", innodbTable,
                            localPartitions.get(j)));
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        threadPool.awaitTermination(3, TimeUnit.MINUTES);
    }
}
