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

package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.google.common.truth.Truth.assertWithMessage;

public class PauseDDLTest extends DDLBaseNewDBTestCase {

    private static final int TABLE_COUNT = 10;

    public PauseDDLTest(boolean schema) {
        this.crossSchema = schema;
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Parameterized.Parameters(name = "{index}:crossSchema={0}")
    public static List<Object[]> initParameters() {
        return Arrays
            .asList(new Object[][] {{false}});
    }

    @Test
    public void testPauseDDL() throws InterruptedException {

        for (int i = 0; i < TABLE_COUNT; i++) {
            String innodbTable = "table_" + i;
            String createSql = String.format("CREATE TABLE %s (\n" +
                "    id bigint NOT NULL AUTO_INCREMENT,\n" +
                "    gmt_modified DATETIME NOT NULL,\n" +
                "    PRIMARY KEY (id, gmt_modified)\n" +
                ")\n" +
                "PARTITION BY HASH(id)\n" +
                "PARTITIONS 4\n" +
                "LOCAL PARTITION BY RANGE (gmt_modified)\n" +
                "STARTWITH '2021-01-01'\n" +
                "INTERVAL 1 MONTH\n" +
                "EXPIRE AFTER 3\n" +
                "PRE ALLOCATE 3\n" +
                "PIVOTDATE NOW();", innodbTable);
            JdbcUtil.executeSuccess(tddlConnection, createSql);
        }

        // create ddl
        ExecutorService threadPool = Executors.newCachedThreadPool();
        CountDownLatch latch = new CountDownLatch(TABLE_COUNT);
        for (int i = 0; i < TABLE_COUNT; i++) {
            final int tableIndex = i;
            threadPool.submit(() -> {
                String innodbTable = "table_" + tableIndex;
                String ddl = "/*+TDDL:cmd_extra(FP_HIJACK_DDL_JOB='10,4,30')*/ unarchive table " + innodbTable;
                try (Connection connection = getPolardbxConnection();
                    Statement statement = connection.createStatement()) {
                    statement.execute(ddl);
                } catch (SQLException ignored) {
                } finally {
                    latch.countDown();
                }
            });
        }

        Thread.sleep(3000);
        //  find all ddl
        boolean findQueued = false;
        List<Long> jobs = new ArrayList<>();
        try (ResultSet rs = JdbcUtil.executeQuery("show ddl", tddlConnection)) {
            while (rs.next()) {
                jobs.add(Long.parseLong(rs.getString("JOB_ID")));
                if (DdlState.valueOf(rs.getString("STATE")) == DdlState.QUEUED) {
                    findQueued = true;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        if (!findQueued) {
            return;
        }

        // pause all ddl
        try (Statement statement = tddlConnection.createStatement()) {
            for (long job : jobs) {
                statement.execute("pause ddl " + job);
            }
        } catch (SQLException ignore) {
            // ignore exception as the ddl maybe finished
        }
        boolean success = latch.await(1, TimeUnit.MINUTES);
        assertWithMessage("Can't finish ddl in 1 min!").that(success).isTrue();

        // find all paused ddl
        jobs.clear();
        Thread.sleep(5000);
        try (ResultSet rs = JdbcUtil.executeQuery("show ddl", tddlConnection)) {
            while (rs.next()) {
                if (DdlState.TERMINATED.contains(DdlState.valueOf(rs.getString("STATE")))) {
                    jobs.add(Long.parseLong(rs.getString("JOB_ID")));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        assertWithMessage("at least one ddl should be paused").that(jobs.size()).isGreaterThan(0);

        // continue all puased ddl
        for (long job : jobs) {
            threadPool.submit(() -> {
                try (Connection connection = getPolardbxConnection();
                    Statement statement = connection.createStatement()) {
                    statement.execute("continue ddl " + job);
                } catch (SQLException ignored) {
                }
            });
        }

        Thread.sleep(3000);
        // wait all ddl to be finished
        long waitTime = System.currentTimeMillis();
        while (true) {
            int cnt = 0;
            try (ResultSet rs = JdbcUtil.executeQuery("show ddl", tddlConnection)) {
                while (rs.next()) {
                    String state = rs.getString("STATE");
                    assertWithMessage("ddl shouldn't be " + state).
                        that(DdlState.RUNNABLE.contains(DdlState.valueOf(state))
                            || DdlState.FINISHED.contains(DdlState.valueOf(state))).isTrue();
                    cnt++;
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            if (cnt > 0) {
                Thread.sleep(5000L);
            }
            if (System.currentTimeMillis() - waitTime > 2 * 60 * 1000L) {
                throw new RuntimeException("Timeout before ddl finished!");
            }
            if (cnt == 0) {
                break;
            }
        }
    }
}