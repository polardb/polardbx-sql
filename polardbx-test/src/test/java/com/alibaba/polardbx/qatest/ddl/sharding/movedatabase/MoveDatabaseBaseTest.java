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

package com.alibaba.polardbx.qatest.ddl.sharding.movedatabase;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.ddl.sharding.repartition.RepartitionBaseTest;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.IntStream;

public class MoveDatabaseBaseTest extends DDLBaseNewDBTestCase {

    protected Map<String, List<String>> storageGroupInfo = new HashMap<>();

    protected Map<String, String> groupToStorageIdMap = new HashMap<>();

    protected Set<String> groupNames = new HashSet<>();

    protected List<String> storageIDs = new ArrayList<>();

    public MoveDatabaseBaseTest(String currentDb) {
        this.currentDb = currentDb;
    }

    public String getCurrentDb() {
        return currentDb;
    }

    protected String currentDb;

    protected static class DDLRequest {
        public void executeDdl() throws Exception {
            // do nothing
        }
    }

    public void initDatasourceInfomation(String database) {
        String tddlSql = "show datasources";
        storageGroupInfo.clear();
        groupNames.clear();
        storageIDs.clear();
        try {
            PreparedStatement stmt = JdbcUtil.preparedStatement(tddlSql, getPolardbxDirectConnection(database));
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                String sid = rs.getString("STORAGE_INST_ID");
                String groupName = rs.getString("GROUP");
                if (groupName.equalsIgnoreCase("MetaDB")
                    || groupName.equalsIgnoreCase("INFORMATION_SCHEMA_SINGLE_GROUP")) {
                    continue;
                }
                int writeWeight = rs.getInt("WRITE_WEIGHT");
                if (writeWeight <= 0) {
                    continue;
                }
                groupNames.add(groupName);
                if (!storageGroupInfo.containsKey(sid)) {
                    storageGroupInfo.put(sid, new ArrayList<>());
                }
                storageGroupInfo.get(sid).add(groupName);
                groupToStorageIdMap.put(groupName, sid);
            }
            rs.close();
            stmt.close();
        } catch (Throwable e) {
            e.printStackTrace();
        }
        storageGroupInfo.forEach((k, v) -> storageIDs.add(k));
    }

    protected void doInsertWhileDDL(String insertSqlTemplate, int threadCount, DDLRequest ddlRequest) throws Exception {
        final ExecutorService dmlPool = new ThreadPoolExecutor(
            threadCount,
            threadCount,
            0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());

        final AtomicBoolean allStop = new AtomicBoolean(false);
        Function<Connection, Integer> call = connection -> {
            // List<Pair< sql, error_message >>
            List<Pair<String, Exception>> failedList = new ArrayList<>();
            try {
                return gsiExecuteUpdate(connection, mysqlConnection, insertSqlTemplate, failedList, false, false);
            } catch (SQLSyntaxErrorException e) {
                if (StringUtils.contains(e.toString(), "ERR_TABLE_NOT_EXIST")) {
                    return 0;
                } else {
                    throw GeneralUtil.nestedException(e);
                }
            } catch (AssertionError e) {
                if (StringUtils.contains(e.toString(), "Communications link failure")) {
                    return 0;
                } else {
                    throw GeneralUtil.nestedException(e);
                }
            }
        };

        List<Future> inserts = new ArrayList<>();

        IntStream.range(0, threadCount).forEach(
            i -> inserts.add(dmlPool.submit(new RepartitionBaseTest.InsertRunner(allStop, call, getCurrentDb())))
        );

        try {
            TimeUnit.MILLISECONDS.sleep(1000);
        } catch (InterruptedException e) {
            // do nothing
        }

        ddlRequest.executeDdl();

        allStop.set(true);
        dmlPool.shutdown();

        for (Future future : inserts) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void pauseDDL(Connection conn, String schema, String ddl_stmt) throws SQLException {
        // pause the ddl job
        String findDDL = String.format("select job_id from metadb.ddl_engine where schema_name = '%s' "
            + "and ddl_stmt = '%s'", schema, ddl_stmt);
        ResultSet resultSet = JdbcUtil.executeQuery(findDDL, conn);
        Assert.assertTrue(resultSet.next());
        long id = resultSet.getLong(1);
        JdbcUtil.executeQuery("pause ddl " + id, conn);
    }

    public static void continueDDL(Connection conn, String schema, String ddl_stmt) throws SQLException {
        // pause the schedule job
        String findTTL = String.format("select job_id from metadb.ddl_engine where schema_name = '%s' "
            + "and ddl_stmt = '%s'", schema, ddl_stmt);
        ResultSet resultSet = JdbcUtil.executeQuery(findTTL, conn);
        Assert.assertTrue(resultSet.next());
        long id = resultSet.getLong(1);
        JdbcUtil.executeQuery("continue ddl " + id, conn);
    }
}
