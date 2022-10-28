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

package com.alibaba.polardbx.qatest.ddl.auto.partition;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class ConcurrentCreateTableTest extends PartitionTestBase {

    @Test
    public void createTableTest() {
        int parallel = 10;
        String tableNamePrefix = "tttt";
        executeConcurrentTasks(tableNamePrefix, 2, "", -1, CreateType.CREATE_TABLE, parallel);
        for (int i = 0; i < parallel - 1; i++) {
            Assert.assertEquals("tableGroup should be the same", getTableGroupId(tableNamePrefix + i).longValue(),
                getTableGroupId(tableNamePrefix + (i + 1)).longValue());
        }
    }

    @Test
    public void createTableIndex() {
        int parallel = 10;
        String tableNamePrefix2 = "ttttx";
        String indexName = "ggggx1";

        executeConcurrentTasks(tableNamePrefix2, -1, "", -1, CreateType.CREATE_TABLE, parallel);
        executeConcurrentTasks(tableNamePrefix2, 1, indexName, 3, CreateType.CREATE_GSI, parallel);
        String query =
            "select count(distinct group_id)  from metadb.table_partitions where table_schema='%s' and table_name like '%s' and part_level='0';";
        query = String.format(query, tddlDatabase1, "%" + indexName + "%");
        List<List<Object>> rs = JdbcUtil.getAllResult(JdbcUtil.executeQuery(query, tddlConnection));
        Assert.assertEquals("all the GSI should be in the same tablegroup",
            ((JdbcUtil.MyNumber) (rs.get(0).get(0))).getNumber().intValue(), 1);

    }

    @Test
    public void alterTableAddIndex() {
        int parallel = 10;
        String tableNamePrefix2 = "ttttttx";
        String indexName = "gggggxx1";

        executeConcurrentTasks(tableNamePrefix2, -1, indexName, -1, CreateType.CREATE_TABLE, parallel);
        executeConcurrentTasks(tableNamePrefix2, 1, indexName, 4, CreateType.ALTER_TABLE_ADD_GSI, parallel);
        String query =
            "select count(distinct group_id)  from metadb.table_partitions where table_schema='%s' and table_name like '%s' and part_level='0';";
        query = String.format(query, tddlDatabase1, "%" + indexName + "%");
        List<List<Object>> rs = JdbcUtil.getAllResult(JdbcUtil.executeQuery(query, tddlConnection));
        Assert.assertEquals("all the GSI should be in the same tablegroup",
            ((JdbcUtil.MyNumber) (rs.get(0).get(0))).getNumber().intValue(), 1);

    }

    @Test
    public void createTableWithGsiTest() {
        int parallel = 10;
        String tableNamePrefix2 = "tttttxx";
        String indexName = "ggggxxx1";
        executeConcurrentTasks(tableNamePrefix2, -1, indexName, 5, CreateType.CREATE_TABLE_WITH_GSI, parallel);
        String query =
            "select count(distinct group_id)  from metadb.table_partitions where table_schema='%s' and table_name like '%s' and part_level='0';";
        query = String.format(query, tddlDatabase1, "%" + indexName + "%");
        List<List<Object>> rs = JdbcUtil.getAllResult(JdbcUtil.executeQuery(query, tddlConnection));
        Assert.assertEquals("all the GSI should be in the same tablegroup",
            ((JdbcUtil.MyNumber) (rs.get(0).get(0))).getNumber().intValue(), 1);
    }

    public void executeConcurrentTasks(String tableNamePrefix, int tbPartitions, String indexName, int indPartitions,
                                       CreateType type,
                                       int parallel) {
        ExecutorService ddlPool = Executors.newFixedThreadPool(parallel);
        final List<Future> tasks = new ArrayList<>();

        try {
            Set<String> ignoreError = new HashSet<>();
            for (int i = 0; i < parallel; i++) {
                int partNum = tbPartitions;
                if (tbPartitions < 1) {
                    partNum = ThreadLocalRandom.current().nextInt(3, 8 + 1);
                }
                TaskRunner taskRunner = new TaskRunner(tableNamePrefix + i, partNum, indexName, indPartitions, type);
                Consumer<String> doDdlFunc = (sql) -> {
                    final Connection conn = getPolardbxConnection();
                    executeSql(tddlDatabase1, sql, conn, ignoreError);
                    try {
                        conn.close();
                    } catch (Exception ex) {

                    }
                };
                taskRunner.setDoDdlFunc(doDdlFunc);
                tasks.add(ddlPool.submit(taskRunner));
            }
            for (Future future : tasks) {
                try {
                    future.get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        } finally {
            ddlPool.shutdown();
        }
    }

    static public void executeSql(String dbName, String sql, Connection connection, Set<String> errIgnored) {
        JdbcUtil.executeUpdate(connection, "use " + dbName);
        JdbcUtil.executeUpdateSuccessIgnoreErr(connection, sql, errIgnored);
    }

    private static class TaskRunner implements Runnable {

        private Consumer<String> doDdlFunc;
        private final String tableName;
        private final CreateType type;
        private final String indexName;
        private final int tbPartitions;
        private final int indPartitions;
        private final String CREATE_TABLE_WITH_GSI_TEMPLATE =
            "create table %s (a int, b bigint, global index %s(b) partition by key(b) partitions %d) partition by key(a) partitions %d";
        private final String CREATE_TABLE_TEMPLATE =
            "create table %s (a int, b bigint) partition by key(a) partitions %d";
        private final String CREATE_INDEX_TEMPLATE =
            "create global index %s on %s(a) partition by key(a) partitions %d";
        private final String ADD_INDEX_TEMPLATE =
            "alter table %s add global index %s(b) partition by key(b) partitions %d";

        public TaskRunner(String tableName, int tbPartitions, String indexName, int indPartitions, CreateType type) {
            this.indPartitions = indPartitions;
            this.indexName = indexName;
            this.tableName = tableName;
            this.tbPartitions = tbPartitions;
            this.type = type;
        }

        public void setDoDdlFunc(Consumer<String> doDdlFunc) {
            this.doDdlFunc = doDdlFunc;
        }

        @Override
        public void run() {
            try {
                String sql;
                switch (type) {
                case CREATE_TABLE:
                    sql = String.format(CREATE_TABLE_TEMPLATE, tableName, tbPartitions);
                    break;
                case CREATE_GSI:
                    sql = String.format(CREATE_INDEX_TEMPLATE, indexName, tableName, indPartitions);
                    break;
                case CREATE_TABLE_WITH_GSI:
                    sql = String.format(CREATE_TABLE_WITH_GSI_TEMPLATE, tableName, indexName, indPartitions,
                        tbPartitions);
                    break;
                case ALTER_TABLE_ADD_GSI:
                    sql = String.format(ADD_INDEX_TEMPLATE, tableName, indexName, indPartitions);
                    break;
                default:
                    sql = null;
                    Assert.assertTrue(false);
                }
                doDdlFunc.accept(sql);
            } catch (Exception e) {
                throw new RuntimeException("Task failed!", e);
            }

        }

    }

    public enum CreateType {

        CREATE_TABLE(0),
        CREATE_TABLE_WITH_GSI(1),
        CREATE_GSI(2),
        ALTER_TABLE_ADD_GSI(3);

        private final int value;

        CreateType(int value) {
            this.value = value;
        }

        public static CreateType from(int value) {
            switch (value) {
            case 0:
                return CREATE_TABLE;
            case 1:
                return CREATE_TABLE_WITH_GSI;
            case 2:
                return CREATE_GSI;
            case 3:
                return ALTER_TABLE_ADD_GSI;
            default:
                return null;
            }
        }

        public int getValue() {
            return value;
        }
    }
}
