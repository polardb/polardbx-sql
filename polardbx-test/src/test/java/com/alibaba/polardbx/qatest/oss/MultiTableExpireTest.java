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
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
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

        long start = Timestamp.valueOf("2022-06-01 00:00:00").getTime();
        long end = Timestamp.valueOf("2022-06-20 00:00:00").getTime();
        try (Connection connection = getPolardbxConnection(testDataBase)) {

            for (int tableIndex = 1; tableIndex < TABLE_COUNT; tableIndex++) {

                String innodbTable = "t_order_" + tableIndex;
                String ossTable = "t_order_" + tableIndex + "_oss";
                Statement statement = connection.createStatement();
                statement.execute("DROP TABLE IF EXISTS " + innodbTable);
                statement.execute("CREATE TABLE " + innodbTable + " (\n"
                    + "    id bigint NOT NULL AUTO_INCREMENT,\n"
                    + "    gmt_modified DATETIME NOT NULL,\n"
                    + "    PRIMARY KEY (id, gmt_modified)\n"
                    + ")\n"
                    + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
                    + "STARTWITH '2022-06-01'\n"
                    + "INTERVAL 1 DAY\n"
                    + "EXPIRE AFTER 7\n"
                    + "PRE ALLOCATE 3;\n");

                statement.execute(
                    "CREATE TABLE " + ossTable + " LIKE " + innodbTable + " ENGINE = '" + engine.name() + "' ARCHIVE_MODE = 'TTL';");

                for (int i = 0; i < 1000; i++) {
                    PreparedStatement preparedStatement =
                        connection.prepareStatement("insert into " + innodbTable + " (gmt_modified) values (?)");
                    for (int j = 0; j < 100; j++) {
                        long time = start + (int) (Math.random() * ((end - start) + 1));
                        String randomTime = new Timestamp(time).toString();
                        preparedStatement.setString(1, randomTime);
                        preparedStatement.addBatch();
                    }
                    preparedStatement.executeBatch();
                }
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
                    Statement statement = connection.createStatement();
                    statement.execute("ALTER TABLE " + innodbTable + " EXPIRE LOCAL PARTITION 'p20220601'");
                    statement.execute("ALTER TABLE " + innodbTable + " EXPIRE LOCAL PARTITION 'p20220602'");
                    statement.execute("ALTER TABLE " + innodbTable + " EXPIRE LOCAL PARTITION 'p20220603'");
                    statement.execute("ALTER TABLE " + innodbTable + " EXPIRE LOCAL PARTITION 'p20220604'");
                    statement.execute("ALTER TABLE " + innodbTable + " EXPIRE LOCAL PARTITION 'p20220605'");
                    statement.execute("ALTER TABLE " + innodbTable + " EXPIRE LOCAL PARTITION 'p20220606'");
                    statement.execute("ALTER TABLE " + innodbTable + " EXPIRE LOCAL PARTITION 'p20220607'");
                    statement.execute("ALTER TABLE " + innodbTable + " EXPIRE LOCAL PARTITION 'p20220608'");
                    statement.execute("ALTER TABLE " + innodbTable + " EXPIRE LOCAL PARTITION 'p20220609'");
                    statement.execute("ALTER TABLE " + innodbTable + " EXPIRE LOCAL PARTITION 'p20220610'");
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        threadPool.awaitTermination(1, TimeUnit.MINUTES);
    }
}
