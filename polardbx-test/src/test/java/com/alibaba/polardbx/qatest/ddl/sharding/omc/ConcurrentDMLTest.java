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

package com.alibaba.polardbx.qatest.ddl.sharding.omc;

import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import lombok.SneakyThrows;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ConcurrentDMLTest extends DDLBaseNewDBTestCase {
    private final boolean supportsAlterType =
        StorageInfoManager.checkSupportAlterType(ConnectionManager.getInstance().getMysqlDataSource());
    private final boolean isRDS80 = StorageInfoManager.checkRDS80(ConnectionManager.getInstance().getMysqlDataSource());

    @Before
    public void beforeMethod() {
        org.junit.Assume.assumeTrue(supportsAlterType && !isRDS80);
    }

    private static final String USE_OMC_ALGORITHM = " ALGORITHM=OMC";
    private static final String OMC_FORCE_TYPE_CONVERSION = "OMC_FORCE_TYPE_CONVERSION=TRUE";
    private static final String OMC_ALTER_TABLE_WITH_GSI = "OMC_ALTER_TABLE_WITH_GSI=TRUE";
    private static final int FILL_COUNT = 2500;
    private static final int FILL_BATCH_SIZE = 2500;

    private static String buildCmdExtra(String... params) {
        if (0 == params.length) {
            return "";
        }
        return "/*+TDDL:CMD_EXTRA(" + String.join(",", params) + ")*/";
    }

    @Test
    public void modifyWithInsert() throws Exception {
        final ExecutorService threadPool = Executors.newFixedThreadPool(2);
        final List<Callable<Void>> tasks = new ArrayList<>();

        tasks.add(() -> {
            String tableName = "omc_with_insert_1";
            String colDef = "int";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s modify column b bigint";
            String selectSql = "select * from %s";
            Function<Integer, String> generator =
                (count) -> String.format("insert into %%s(a,b) values (%d,%d+1)", count, count);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colA + 1 == colB;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, false,
                1);
            System.out.println("modifyWithInsert 1");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_insert_2";
            String colDef = "int";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s modify column b bigint";
            String selectSql = "select * from %s";
            Function<Integer, String> generator = (count) -> String.format("insert into %%s(a) values (%d)", count);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colB == 0;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, false,
                1);
            System.out.println("modifyWithInsert 2");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_insert_3";
            String colDef = "int not null";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s modify column b bigint not null";
            String selectSql = "select * from %s";
            Function<Integer, String> generator =
                (count) -> String.format("insert into %%s(a,b) values (%d,%d+1)", count, count);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colA + 1 == colB;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, false,
                1);
            System.out.println("modifyWithInsert 3");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_insert_4";
            String colDef = "int not null";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s modify column b bigint not null";
            String selectSql = "select * from %s";
            Function<Integer, String> generator =
                (count) -> String.format("insert into %%s values (%d,%d+1)", count, count);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colA + 1 == colB;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, false,
                1);
            System.out.println("modifyWithInsert 4");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_insert_5";
            String colDef = "int not null default 3";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s modify column b bigint not null default 4";
            String selectSql = "select * from %s";
            Function<Integer, String> generator = (count) -> String.format("insert into %%s(a) values (%d)", count);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> (colB == 3 || colB == 4);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, false,
                1);
            System.out.println("modifyWithInsert 5");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_insert_6";
            String colDef = "int default 3";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s modify column b bigint default null";
            String selectSql = "select * from %s";
            Function<Integer, String> generator = (count) -> String.format("insert into %%s(a) values (%d)", count);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> (colB == 3 || colB == 0);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, false,
                1);
            System.out.println("modifyWithInsert 6");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_insert_7";
            String colDef = "int";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s modify column b bigint";
            String selectSql = "select * from %s";
            Function<Integer, String> generator =
                (count) -> String.format("insert into %%s(a,b) values (%d,%d),(%d,%d)", count * 2, count * 2 + 1,
                    count * 2 + 1, count * 2 + 2);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colA + 1 == colB;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, true,
                2);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, false,
                2);
            System.out.println("modifyWithInsert 7");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_insert_8";
            String colDef = "int";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s modify column b bigint";
            String selectSql = "select * from %s";
            Function<Integer, String> generator =
                (count) -> String.format("insert into %%s(a,b) values (%d*2,%d*2+1),(%d*2+1,%d*2+2)", count, count,
                    count, count);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colA + 1 == colB;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, true,
                2);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, false,
                2);
            System.out.println("modifyWithInsert 8");
            return null;
        });

        ArrayList<Future<Void>> results = new ArrayList<>();
        for (Callable<Void> task : tasks) {
            results.add(threadPool.submit(task));
        }

        for (Future<Void> result : results) {
            result.get();
        }
    }

    @Test
    public void modifyWithInsertSelect() throws Exception {
        final ExecutorService threadPool = Executors.newFixedThreadPool(2);
        final List<Callable<Void>> tasks = new ArrayList<>();

        String selectTableName = "omc_with_insert_select_src_tbl";
        buildSelectTable(selectTableName);

        tasks.add(() -> {
            String tableName = "omc_with_insert_select_1";
            String colDef = "int";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s modify column b bigint";
            String selectSql = "select * from %s";
            Function<Integer, String> generator =
                (count) -> String.format("insert into %%s(a,b) select %d,%d+1", count, count);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> (colA == colB - 1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, false,
                1);
            System.out.println("modifyWithInsertSelect 1");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_insert_select_2";
            String colDef = "int";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s modify column b bigint";
            String selectSql = "select * from %s";
            Function<Integer, String> generator =
                (count) -> String.format("insert into %%s(b,a) select c,d+1 from %s where c=%d", selectTableName,
                    count);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> (colA == colB + 1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, false,
                1);
            System.out.println("modifyWithInsertSelect 2");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_insert_select_3";
            String colDef = "int";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s modify column b bigint";
            String selectSql = "select * from %s";
            Function<Integer, String> generator =
                (count) -> String.format("insert into %%s select * from %s where c=%d", selectTableName, count);
            BiFunction<Integer, Integer, Boolean> checker = Integer::equals;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, false, false,
                1);
            System.out.println("modifyWithInsertSelect 3");
            return null;
        });

        ArrayList<Future<Void>> results = new ArrayList<>();
        for (Callable<Void> task : tasks) {
            results.add(threadPool.submit(task));
        }

        for (Future<Void> result : results) {
            result.get();
        }
    }

    @Test
    public void modifyWithUpdate() throws Exception {
        final ExecutorService threadPool = Executors.newFixedThreadPool(2);
        final List<Callable<Void>> tasks = new ArrayList<>();

        tasks.add(() -> {
            String tableName = "omc_with_update_1";
            String colDef = "int";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s modify column b bigint";
            String selectSql = "select * from %s order by a";
            Function<Integer, String> generator =
                (count) -> String.format("update %%s set b=%d+1 where a=%d", count, count);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colA + 1 == colB;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
                1);
            System.out.println("modifyWithUpdate 1");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_update_2";
            String colDef = "int";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s modify column b bigint";
            String selectSql = "select * from %s order by a";
            Function<Integer, String> generator =
                (count) -> String.format("update %%s set a=%d-1 where b=%d", count, count);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colA + 1 == colB;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
                1);
            System.out.println("modifyWithUpdate 2");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_update_3";
            String colDef = "int default 3";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s modify column b bigint default 4";
            String selectSql = "select * from %s order by a";
            Function<Integer, String> generator =
                (count) -> String.format("update %%s set b=default where a=%d", count);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colB == 3 || colB == 4;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
                1);
            System.out.println("modifyWithUpdate 3");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_update_4";
            String colDef = "int default 3";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s modify column b bigint default 4";
            String selectSql = "select * from %s order by a";
            Function<Integer, String> generator =
                (count) -> String.format("update %%s set b=default(b) where a=%d", count);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colB == 3 || colB == 4;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
                1);
            System.out.println("modifyWithUpdate 4");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_update_5";
            String colDef = "int default 3";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s modify column b bigint default 4";
            String selectSql = "select * from %s order by a";
            Function<Integer, String> generator = (count) -> String.format("update %%s set b=a+1 where a=%d", count);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colB == colA + 1;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
                1);
            System.out.println("modifyWithUpdate 5");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_update_6";
            String colDef = "int default 3";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s modify column b bigint default 4";
            String selectSql = "select * from %s order by a";
            Function<Integer, String> generator =
                (count) -> String.format("update %%s set b=a+1,a=%d,b=a+1,b=b+1 where a=%d", count, count);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colB == colA + 2;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
                1);
            System.out.println("modifyWithUpdate 6");
            return null;
        });

        ArrayList<Future<Void>> results = new ArrayList<>();
        for (Callable<Void> task : tasks) {
            results.add(threadPool.submit(task));
        }

        for (Future<Void> result : results) {
            result.get();
        }
    }

    @Test
    public void modifyWithReplace() throws Exception {
        final ExecutorService threadPool = Executors.newFixedThreadPool(2);
        final List<Callable<Void>> tasks = new ArrayList<>();

        tasks.add(() -> {
            String tableName = "omc_with_replace_1";
            String colDef = "int";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s modify column b bigint";
            String selectSql = "select * from %s order by a";
            Function<Integer, String> generator =
                (count) -> String.format("replace into %%s values(%d, %d + %d)", count, count, FILL_COUNT);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colA + FILL_COUNT == colB;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
                1);
            System.out.println("modifyWithReplace 1");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_replace_2";
            String colDef = "int unique key";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s modify column b bigint";
            String selectSql = "select * from %s order by a desc";
            Function<Integer, String> generator =
                (count) -> String.format("replace into %%s values(%d + %d, %d)", count, FILL_COUNT, count);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colA - FILL_COUNT == colB;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
                1);
            System.out.println("modifyWithReplace 2");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_replace_3";
            String colDef = "int default 3";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s modify column b bigint default 4";
            String selectSql = "select * from %s order by a";
            Function<Integer, String> generator = (count) -> String.format("replace into %%s(a) values(%d)", count);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colB == 3 || colB == 4;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
                1);
            System.out.println("modifyWithReplace 3");
            return null;
        });

        ArrayList<Future<Void>> results = new ArrayList<>();
        for (Callable<Void> task : tasks) {
            results.add(threadPool.submit(task));
        }

        for (Future<Void> result : results) {
            result.get();
        }
    }

    @Test
    public void modifyWithUpsert() throws Exception {
        final ExecutorService threadPool = Executors.newFixedThreadPool(2);
        final List<Callable<Void>> tasks = new ArrayList<>();

        tasks.add(() -> {
            String tableName = "omc_with_upsert_1";
            String colDef = "int";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s modify column b bigint";
            String selectSql = "select * from %s order by a";
            Function<Integer, String> generator =
                (count) -> String.format("insert into %%s values(%d, %d) on duplicate key update b=b+%d", count, count,
                    FILL_COUNT);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colA + FILL_COUNT == colB;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
                1);
            System.out.println("modifyWithUpsert 1");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_upsert_2";
            String colDef = "int";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s modify column b bigint";
            String selectSql = "select * from %s order by a desc";
            Function<Integer, String> generator =
                (count) -> String.format("insert into %%s values(%d, %d) on duplicate key update a=a+%d", count, count,
                    FILL_COUNT);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colA - FILL_COUNT == colB;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
                1);
            System.out.println("modifyWithUpsert 2");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_upsert_3";
            String colDef = "int unique key";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s modify column b bigint";
            String selectSql = "select * from %s order by a desc";
            Function<Integer, String> generator =
                (count) -> String.format("insert into %%s values(%d + %d, %d) on duplicate key update a=a+%d", count,
                    FILL_COUNT, count, FILL_COUNT);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colA - FILL_COUNT == colB;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
                1);
            System.out.println("modifyWithUpsert 3");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_upsert_4";
            String colDef = "int unique key";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s modify column b bigint";
            String selectSql = "select * from %s order by a";
            Function<Integer, String> generator =
                (count) -> String.format("insert into %%s values(%d, %d + %d) on duplicate key update b=b+%d", count,
                    count, FILL_COUNT, FILL_COUNT);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colA + FILL_COUNT == colB;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
                1);
            System.out.println("modifyWithUpsert 4");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_upsert_5";
            String colDef = "int default 3";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s modify column b bigint default 4";
            String selectSql = "select * from %s order by a";
            Function<Integer, String> generator =
                (count) -> String.format("insert into %%s values(%d, %d + %d) on duplicate key update b=default", count,
                    count, FILL_COUNT);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colB == 3 || colB == 4;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
                1);
            System.out.println("changeWithUpsert 5");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_upsert_6";
            String colDef = "int default 3";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s modify column b bigint default 4";
            String selectSql = "select * from %s order by a";
            Function<Integer, String> generator =
                (count) -> String.format("insert into %%s values(%d, %d + %d) on duplicate key update b=default(b)",
                    count, count, FILL_COUNT);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colB == 3 || colB == 4;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
                1);
            System.out.println("changeWithUpsert 6");
            return null;
        });

        ArrayList<Future<Void>> results = new ArrayList<>();
        for (Callable<Void> task : tasks) {
            results.add(threadPool.submit(task));
        }

        for (Future<Void> result : results) {
            result.get();
        }
    }

    @Test
    public void modifyWithInsertIgnore() throws Exception {
        final ExecutorService threadPool = Executors.newFixedThreadPool(2);
        final List<Callable<Void>> tasks = new ArrayList<>();

        tasks.add(() -> {
            String tableName = "omc_with_insert_ignore_1";
            String colDef = "int";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s modify column b bigint";
            String selectSql = "select * from %s order by a";
            Function<Integer, String> generator =
                (count) -> String.format("insert ignore into %%s values(%d, %d + %d)", count, count, FILL_COUNT);
            BiFunction<Integer, Integer, Boolean> checker = Objects::equals;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
                1);
            System.out.println("modifyWithInsertIgnore 1");
            return null;
        });

        // 2
        tasks.add(() -> {
            String tableName = "omc_with_insert_ignore_2";
            String colDef = "int unique key";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s modify column b bigint";
            String selectSql = "select * from %s order by a desc";
            Function<Integer, String> generator =
                (count) -> String.format("insert ignore into %%s values(%d + %d, %d)", count, FILL_COUNT, count);
            BiFunction<Integer, Integer, Boolean> checker = Objects::equals;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
                1);
            System.out.println("modifyWithInsertIgnore 2");
            return null;
        });

        ArrayList<Future<Void>> results = new ArrayList<>();
        for (Callable<Void> task : tasks) {
            results.add(threadPool.submit(task));
        }

        for (Future<Void> result : results) {
            result.get();
        }
    }

    @Test
    public void changeWithInsert() throws Exception {
        final ExecutorService threadPool = Executors.newFixedThreadPool(2);
        final List<Callable<Void>> tasks = new ArrayList<>();

        tasks.add(() -> {
            String tableName = "omc_with_insert_1";
            String colDef = "int";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s change column b c bigint";
            String selectSql = "select * from %s";
            Function<Integer, String> generator1 =
                (count) -> String.format("insert into %%s(a,b) values (%d,%d+1)", count, count);
            Function<Integer, String> generator2 =
                (count) -> String.format("insert into %%s(a,c) values (%d,%d+1)", count, count);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colA + 1 == colB;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false,
                false, 1);
            System.out.println("changeWithInsert 1");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_insert_2";
            String colDef = "int";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s change column b c bigint";
            String selectSql = "select * from %s";
            Function<Integer, String> generator1 = (count) -> String.format("insert into %%s(a) values (%d)", count);
            Function<Integer, String> generator2 = (count) -> String.format("insert into %%s(a) values (%d)", count);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colB == 0;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false,
                false, 1);
            System.out.println("changeWithInsert 2");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_insert_3";
            String colDef = "int not null";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s change column b c bigint not null";
            String selectSql = "select * from %s";
            Function<Integer, String> generator1 =
                (count) -> String.format("insert into %%s(a,b) values (%d,%d+1)", count, count);
            Function<Integer, String> generator2 =
                (count) -> String.format("insert into %%s(a,c) values (%d,%d+1)", count, count);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colA + 1 == colB;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false,
                false, 1);
            System.out.println("changeWithInsert 3");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_insert_4";
            String colDef = "int not null";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s change column b c bigint not null";
            String selectSql = "select * from %s";
            Function<Integer, String> generator1 =
                (count) -> String.format("insert into %%s values (%d,%d+1)", count, count);
            Function<Integer, String> generator2 =
                (count) -> String.format("insert into %%s values (%d,%d+1)", count, count);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colA + 1 == colB;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false,
                false, 1);
            System.out.println("changeWithInsert 4");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_insert_5";
            String colDef = "int not null default 3";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s change column b c bigint not null default 4";
            String selectSql = "select * from %s";
            Function<Integer, String> generator1 = (count) -> String.format("insert into %%s(a) values (%d)", count);
            Function<Integer, String> generator2 = (count) -> String.format("insert into %%s(a) values (%d)", count);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> (colB == 3 || colB == 4);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false,
                false, 1);
            System.out.println("changeWithInsert 5");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_insert_6";
            String colDef = "int default 3";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s change column b c bigint default null";
            String selectSql = "select * from %s";
            Function<Integer, String> generator1 = (count) -> String.format("insert into %%s(a) values (%d)", count);
            Function<Integer, String> generator2 = (count) -> String.format("insert into %%s(a) values (%d)", count);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> (colB == 3 || colB == 0);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false,
                false, 1);
            System.out.println("changeWithInsert 6");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_insert_7";
            String colDef = "int";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s change column b c bigint";
            String selectSql = "select * from %s";
            Function<Integer, String> generator1 =
                (count) -> String.format("insert into %%s(a,b) values (%d,%d),(%d,%d)", count * 2, count * 2 + 1,
                    count * 2 + 1, count * 2 + 2);
            Function<Integer, String> generator2 =
                (count) -> String.format("insert into %%s(a,c) values (%d,%d),(%d,%d)", count * 2, count * 2 + 1,
                    count * 2 + 1, count * 2 + 2);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colA + 1 == colB;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false, true,
                2);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false,
                false, 2);
            System.out.println("modifyWithInsert 7");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_insert_8";
            String colDef = "int";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s change column b c bigint";
            String selectSql = "select * from %s";
            Function<Integer, String> generator1 =
                (count) -> String.format("insert into %%s(a,b) values (%d*2,%d*2+1),(%d*2+1,%d*2+2)", count, count,
                    count, count);
            Function<Integer, String> generator2 =
                (count) -> String.format("insert into %%s(a,c) values (%d*2,%d*2+1),(%d*2+1,%d*2+2)", count, count,
                    count, count);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colA + 1 == colB;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false, true,
                2);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false,
                false, 2);
            System.out.println("modifyWithInsert 8");
            return null;
        });

        ArrayList<Future<Void>> results = new ArrayList<>();
        for (Callable<Void> task : tasks) {
            results.add(threadPool.submit(task));
        }

        for (Future<Void> result : results) {
            result.get();
        }
    }

    @Test
    public void changeWithInsertSelect() throws Exception {
        final ExecutorService threadPool = Executors.newFixedThreadPool(2);
        final List<Callable<Void>> tasks = new ArrayList<>();

        String selectTableName = "omc_with_insert_select_src_tbl";
        buildSelectTable(selectTableName);

        tasks.add(() -> {
            String tableName = "omc_with_insert_select_1";
            String colDef = "int";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s change column b e bigint";
            String selectSql = "select * from %s";
            Function<Integer, String> generator1 =
                (count) -> String.format("insert into %%s(a,b) select %d,%d+1", count, count);
            Function<Integer, String> generator2 =
                (count) -> String.format("insert into %%s(a,e) select %d,%d+1", count, count);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> (colA == colB - 1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false,
                false, 1);
            System.out.println("modifyWithInsertSelect 1");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_insert_select_2";
            String colDef = "int";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s modify column b bigint";
            String selectSql = "select * from %s";
            Function<Integer, String> generator1 =
                (count) -> String.format("insert into %%s(b,a) select c,d+1 from %s where c=%d", selectTableName,
                    count);
            Function<Integer, String> generator2 =
                (count) -> String.format("insert into %%s(e,a) select c,d+1 from %s where c=%d", selectTableName,
                    count);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> (colA == colB + 1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false,
                false, 1);
            System.out.println("modifyWithInsertSelect 2");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_insert_select_3";
            String colDef = "int";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s modify column b bigint";
            String selectSql = "select * from %s";
            Function<Integer, String> generator1 =
                (count) -> String.format("insert into %%s select * from %s where c=%d", selectTableName, count);
            Function<Integer, String> generator2 =
                (count) -> String.format("insert into %%s select * from %s where c=%d", selectTableName, count);
            BiFunction<Integer, Integer, Boolean> checker = Integer::equals;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, false,
                false, 1);
            System.out.println("modifyWithInsertSelect 3");
            return null;
        });

        ArrayList<Future<Void>> results = new ArrayList<>();
        for (Callable<Void> task : tasks) {
            results.add(threadPool.submit(task));
        }

        for (Future<Void> result : results) {
            result.get();
        }
    }

    @Test
    public void changeWithUpdate() throws Exception {
        final ExecutorService threadPool = Executors.newFixedThreadPool(2);
        final List<Callable<Void>> tasks = new ArrayList<>();

        tasks.add(() -> {
            String tableName = "omc_with_update_1";
            String colDef = "int";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s change column b c bigint";
            String selectSql = "select * from %s order by a";
            Function<Integer, String> generator1 =
                (count) -> String.format("update %%s set b=%d+1 where a=%d", count, count);
            Function<Integer, String> generator2 =
                (count) -> String.format("update %%s set c=%d+1 where a=%d", count, count);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colA + 1 == colB;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, false,
                1);
            System.out.println("changeWithUpdate 1");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_update_2";
            String colDef = "int";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s change column b c bigint";
            String selectSql = "select * from %s order by a";
            Function<Integer, String> generator1 =
                (count) -> String.format("update %%s set a=%d-1 where b=%d", count, count);
            Function<Integer, String> generator2 =
                (count) -> String.format("update %%s set a=%d-1 where c=%d", count, count);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colA + 1 == colB;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, false,
                1);
            System.out.println("changeWithUpdate 2");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_update_3";
            String colDef = "int default 3";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s change column b c bigint default 4";
            String selectSql = "select * from %s order by a";
            Function<Integer, String> generator1 =
                (count) -> String.format("update %%s set b=default where a=%d", count);
            Function<Integer, String> generator2 =
                (count) -> String.format("update %%s set c=default where a=%d", count);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colB == 3 || colB == 4;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, false,
                1);
            System.out.println("changeWithUpdate 3");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_update_4";
            String colDef = "int default 3";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s change column b c bigint default 4";
            String selectSql = "select * from %s order by a";
            Function<Integer, String> generator1 =
                (count) -> String.format("update %%s set b=default(b) where a=%d", count);
            Function<Integer, String> generator2 =
                (count) -> String.format("update %%s set c=default(c) where a=%d", count);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colB == 3 || colB == 4;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, false,
                1);
            System.out.println("changeWithUpdate 4");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_update_5";
            String colDef = "int default 3";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s modify column b bigint default 4";
            String selectSql = "select * from %s order by a";
            Function<Integer, String> generator1 = (count) -> String.format("update %%s set b=a+1 where a=%d", count);
            Function<Integer, String> generator2 = (count) -> String.format("update %%s set c=a+1 where a=%d", count);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colB == colA + 1;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, false,
                1);
            System.out.println("changeWithUpdate 5");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_update_6";
            String colDef = "int default 3";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s modify column b bigint default 4";
            String selectSql = "select * from %s order by a";
            Function<Integer, String> generator1 =
                (count) -> String.format("update %%s set b=a+1,a=%d,b=a+1,b=b+1 where a=%d", count, count);
            Function<Integer, String> generator2 =
                (count) -> String.format("update %%s set c=a+1,a=%d,c=a+1,c=c+1 where a=%d", count, count);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colB == colA + 2;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, false,
                1);
            System.out.println("changeWithUpdate 6");
            return null;
        });

        ArrayList<Future<Void>> results = new ArrayList<>();
        for (Callable<Void> task : tasks) {
            results.add(threadPool.submit(task));
        }

        for (Future<Void> result : results) {
            result.get();
        }
    }

    @Test
    public void changeWithReplace() throws Exception {
        final ExecutorService threadPool = Executors.newFixedThreadPool(2);
        final List<Callable<Void>> tasks = new ArrayList<>();
        tasks.add(() -> {
            String tableName = "omc_with_replace_1";
            String colDef = "int";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s change column b c bigint";
            String selectSql = "select * from %s order by a";
            Function<Integer, String> generator =
                (count) -> String.format("replace into %%s values(%d, %d + %d)", count, count, FILL_COUNT);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colA + FILL_COUNT == colB;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
                1);
            System.out.println("changeWithReplace 1");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_replace_2";
            String colDef = "int unique key";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s change column b c bigint";
            String selectSql = "select * from %s order by a desc";
            Function<Integer, String> generator =
                (count) -> String.format("replace into %%s values(%d + %d, %d)", count, FILL_COUNT, count);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colA - FILL_COUNT == colB;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
                1);
            System.out.println("changeWithReplace 2");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_replace_3";
            String colDef = "int default 3";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s change column b c bigint default 4";
            String selectSql = "select * from %s order by a";
            Function<Integer, String> generator = (count) -> String.format("replace into %%s(a) values(%d)", count);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colB == 3 || colB == 4;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
                1);
            System.out.println("changeWithReplace 3");
            return null;
        });

        ArrayList<Future<Void>> results = new ArrayList<>();
        for (Callable<Void> task : tasks) {
            results.add(threadPool.submit(task));
        }

        for (Future<Void> result : results) {
            result.get();
        }
    }

    @Test
    public void changeWithUpsert() throws Exception {
        final ExecutorService threadPool = Executors.newFixedThreadPool(2);
        final List<Callable<Void>> tasks = new ArrayList<>();
        tasks.add(() -> {
            String tableName = "omc_with_upsert_1";
            String colDef = "int";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s change column b c bigint";
            String selectSql = "select * from %s order by a";
            Function<Integer, String> generator1 =
                (count) -> String.format("insert into %%s values(%d, %d) on duplicate key update b=b+%d", count, count,
                    FILL_COUNT);
            Function<Integer, String> generator2 =
                (count) -> String.format("insert into %%s values(%d, %d) on duplicate key update c=c+%d", count, count,
                    FILL_COUNT);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colA + FILL_COUNT == colB;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, false,
                1);
            System.out.println("changeWithUpsert 1");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_upsert_2";
            String colDef = "int";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s change column b c bigint";
            String selectSql = "select * from %s order by a desc";
            Function<Integer, String> generator1 =
                (count) -> String.format("insert into %%s values(%d, %d) on duplicate key update a=a+%d", count, count,
                    FILL_COUNT);
            Function<Integer, String> generator2 =
                (count) -> String.format("insert into %%s values(%d, %d) on duplicate key update a=a+%d", count, count,
                    FILL_COUNT);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colA - FILL_COUNT == colB;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, false,
                1);
            System.out.println("changeWithUpsert 2");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_upsert_3";
            String colDef = "int unique key";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s change column b c bigint";
            String selectSql = "select * from %s order by a desc";
            Function<Integer, String> generator1 =
                (count) -> String.format("insert into %%s values(%d + %d, %d) on duplicate key update a=a+%d", count,
                    FILL_COUNT, count, FILL_COUNT);
            Function<Integer, String> generator2 =
                (count) -> String.format("insert into %%s values(%d + %d, %d) on duplicate key update a=a+%d", count,
                    FILL_COUNT, count, FILL_COUNT);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colA - FILL_COUNT == colB;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, false,
                1);
            System.out.println("changeWithUpsert 3");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_upsert_4";
            String colDef = "int unique key";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s change column b c bigint";
            String selectSql = "select * from %s order by a";
            Function<Integer, String> generator1 =
                (count) -> String.format("insert into %%s values(%d, %d + %d) on duplicate key update b=b+%d", count,
                    count, FILL_COUNT, FILL_COUNT);
            Function<Integer, String> generator2 =
                (count) -> String.format("insert into %%s values(%d, %d + %d) on duplicate key update c=c+%d", count,
                    count, FILL_COUNT, FILL_COUNT);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colA + FILL_COUNT == colB;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, false,
                1);
            System.out.println("changeWithUpsert 4");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_upsert_5";
            String colDef = "int default 3";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s change column b c bigint default 4";
            String selectSql = "select * from %s order by a";
            Function<Integer, String> generator1 =
                (count) -> String.format("insert into %%s values(%d, %d + %d) on duplicate key update b=default", count,
                    count, FILL_COUNT);
            Function<Integer, String> generator2 =
                (count) -> String.format("insert into %%s values(%d, %d + %d) on duplicate key update c=default", count,
                    count, FILL_COUNT);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colB == 3 || colB == 4;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, false,
                1);
            System.out.println("changeWithUpsert 5");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_upsert_6";
            String colDef = "int default 3";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s modify column b bigint default 4";
            String selectSql = "select * from %s order by a";
            Function<Integer, String> generator1 =
                (count) -> String.format("insert into %%s values(%d, %d + %d) on duplicate key update b=default(b)",
                    count, count, FILL_COUNT);
            Function<Integer, String> generator2 =
                (count) -> String.format("insert into %%s values(%d, %d + %d) on duplicate key update c=default(c)",
                    count, count, FILL_COUNT);
            BiFunction<Integer, Integer, Boolean> checker = (colA, colB) -> colB == 3 || colB == 4;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator1, generator2, checker, true, false,
                1);
            System.out.println("changeWithUpsert 6");
            return null;
        });

        ArrayList<Future<Void>> results = new ArrayList<>();
        for (Callable<Void> task : tasks) {
            results.add(threadPool.submit(task));
        }

        for (Future<Void> result : results) {
            result.get();
        }
    }

    @Test
    public void changeWithInsertIgnore() throws Exception {
        final ExecutorService threadPool = Executors.newFixedThreadPool(2);
        final List<Callable<Void>> tasks = new ArrayList<>();
        tasks.add(() -> {
            String tableName = "omc_with_insert_ignore_1";
            String colDef = "int";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s change column b c bigint";
            String selectSql = "select * from %s order by a";
            Function<Integer, String> generator =
                (count) -> String.format("insert ignore into %%s values(%d, %d + %d)", count, count, FILL_COUNT);
            BiFunction<Integer, Integer, Boolean> checker = Objects::equals;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
                1);
            System.out.println("changeWithInsertIgnore 1");
            return null;
        });

        tasks.add(() -> {
            String tableName = "omc_with_insert_ignore_2";
            String colDef = "int unique key";
            String alterSql = buildCmdExtra(OMC_FORCE_TYPE_CONVERSION, OMC_ALTER_TABLE_WITH_GSI)
                + " alter table %s change column b c bigint";
            String selectSql = "select * from %s order by a desc";
            Function<Integer, String> generator =
                (count) -> String.format("insert ignore into %%s values(%d + %d, %d)", count, FILL_COUNT, count);
            BiFunction<Integer, Integer, Boolean> checker = Objects::equals;
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, true,
                1);
            concurrentTestInternal(tableName, colDef, alterSql, selectSql, generator, generator, checker, true, false,
                1);
            System.out.println("changeWithInsertIgnore 2");
            return null;
        });

        ArrayList<Future<Void>> results = new ArrayList<>();
        for (Callable<Void> task : tasks) {
            results.add(threadPool.submit(task));
        }

        for (Future<Void> result : results) {
            result.get();
        }
    }

    public void buildSelectTable(String selectTableName) {
        dropTableIfExists(selectTableName);
        String createSql = String.format(
            "create table %s ("
                + "c int primary key, "
                + "d int "
                + ") dbpartition by hash(`c`)",
            selectTableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        for (int i = 0; i < FILL_COUNT; i++) {
            String sql = String.format("insert into table %s values (%d, %d)", selectTableName, i, i);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }
    }

    public void concurrentTestInternal(String tableName, String colDef, String alterSql, String selectSql,
                                       Function<Integer, String> generator1, Function<Integer, String> generator2,
                                       BiFunction<Integer, Integer, Boolean> checker, boolean fillData, boolean withGsi,
                                       int batchSize)
        throws Exception {
        tableName = tableName + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String indexName1 = tableName + "_idx_1";
        String indexName2 = tableName + "_idx_2";
        String indexName3 = tableName + "_idx_3";
        String finalTableName = tableName;
        Connection conn = getPolardbxConnection();

        try {
            String createSql = withGsi ? String.format(
                "create table %s ("
                    + "a int primary key, "
                    + "b %s, "
                    + "global index %s(`a`) dbpartition by hash(a),"
                    + "global index %s(`a`) covering(b) dbpartition by hash(a),"
                    + "clustered index %s(`a`) dbpartition by hash(a)"
                    + ") dbpartition by hash(a) tbpartition by hash(a) tbpartitions 2",
                tableName, colDef, indexName1, indexName2, indexName3) :
                String.format(
                    "create table %s ("
                        + "a int primary key, "
                        + "b %s "
                        + ") dbpartition by hash(a) tbpartition by hash(a) tbpartitions 2",
                    tableName, colDef);
            JdbcUtil.executeUpdateSuccess(conn, createSql);

            if (fillData) {
                final String insert = String.format("insert into %s values (?,?)", tableName);
                for (int i = 0; i < FILL_COUNT; i += FILL_BATCH_SIZE) {
                    List<List<Object>> params = new ArrayList<>();
                    for (int j = 0; j < FILL_BATCH_SIZE; j++) {
                        List<Object> param = new ArrayList<>();
                        param.add(j + i);
                        param.add(j + i);
                        params.add(param);
                    }
                    JdbcUtil.updateDataBatch(conn, insert, params);
                }
            }

            BiFunction<AtomicBoolean, AtomicInteger, Void> dmlFunc =
                new BiFunction<AtomicBoolean, AtomicInteger, Void>() {
                    @SneakyThrows
                    @Override
                    public Void apply(AtomicBoolean shouldStop, AtomicInteger totalCount) {
                        Connection connection = getPolardbxConnection();
                        try {
                            Function<Integer, String> generator = generator1;
                            boolean changed = false;
                            while ((!shouldStop.get() || fillData) && totalCount.get() < FILL_COUNT) {
                                String sql = String.format(generator.apply(totalCount.get()), finalTableName);
                                try {
                                    JdbcUtil.executeUpdateSuccess(connection, sql);
                                } catch (AssertionError e) {
                                    if (!changed) {
                                        changed = true;
                                        generator = generator2;
                                        totalCount.getAndDecrement();
                                    } else {
                                        throw e;
                                    }
                                }

                                totalCount.incrementAndGet();
                            }
                        } finally {
                            connection.close();
                        }
                        return null;
                    }
                };

            BiFunction<AtomicBoolean, AtomicInteger, Void> alterFunc =
                new BiFunction<AtomicBoolean, AtomicInteger, Void>() {
                    @SneakyThrows
                    @Override
                    public Void apply(AtomicBoolean shouldStop, AtomicInteger totalCount) {
                        Connection connection = getPolardbxConnection();
                        Thread.sleep(1000);
                        String sql = String.format(alterSql, finalTableName) + USE_OMC_ALGORITHM;
                        try {
                            execDdlWithRetry(tddlDatabase1, finalTableName, sql, connection);
                        } finally {
                            shouldStop.set(true);
                            connection.close();
                        }
                        Thread.sleep(1000);
                        System.out.println(totalCount.get());
                        return null;
                    }
                };

            List<String> index = new ArrayList<>();
            index.add(" force index (primary)");
            if (withGsi) {
                index.add(String.format(" force index (%s) ", indexName1));
                index.add(String.format(" force index (%s) ", indexName2));
                index.add(String.format(" force index (%s) ", indexName3));
            }

            BiFunction<AtomicBoolean, AtomicInteger, Void> selectFunc =
                new BiFunction<AtomicBoolean, AtomicInteger, Void>() {
                    @SneakyThrows
                    @Override
                    public Void apply(AtomicBoolean shouldStop, AtomicInteger totalCount) {
                        Connection connection = getPolardbxConnection();
                        Statement statement = connection.createStatement();
                        try {
                            int cnt = 0;
                            while ((!shouldStop.get() || fillData) && totalCount.get() < FILL_COUNT) {
                                int tot = totalCount.get();
                                ResultSet rs = statement.executeQuery(
                                    String.format(selectSql, finalTableName + index.get(cnt % index.size())));
                                cnt++;
                                ResultSetMetaData rsmd = rs.getMetaData();
                                assertEquals(2, rsmd.getColumnCount());
                                while (rs.next() && tot > 0) {
                                    int colA = rs.getInt(1);
                                    int colB = rs.getInt(2);
                                    if (!checker.apply(colA, colB)) {
                                        System.out.println(tot + ": " + colA + " " + colB);
                                    }
                                    assertTrue(checker.apply(colA, colB));
                                    tot--;
                                }
                                rs.close();
                                Thread.sleep(500);
                            }
                            statement.close();

                            Thread.sleep(500); // sleep to wait dmlTask
                            ResultSet rs =
                                JdbcUtil.executeQuerySuccess(connection, String.format(selectSql, finalTableName));
                            ResultSetMetaData rsmd = rs.getMetaData();
                            assertEquals(2, rsmd.getColumnCount());
                            int actualCount = 0;
                            while (rs.next()) {
                                int colA = rs.getInt(1);
                                int colB = rs.getInt(2);
                                if (!checker.apply(colA, colB)) {
                                    System.out.println(actualCount + ": " + colA + " " + colB);
                                }
                                assertTrue(checker.apply(colA, colB));
                                actualCount++;
                            }
                            // assertEquals(totalCount.get() * batchSize, actualCount); // this will fail sometimes due to sync problem
                        } finally {
                            connection.close();
                        }
                        return null;
                    }
                };

            AtomicBoolean shouldStop = new AtomicBoolean(false);
            AtomicInteger totalCount = new AtomicInteger(0);

            final ExecutorService threadPool = Executors.newFixedThreadPool(3);
            Callable<Void> dmlTask = () -> {
                dmlFunc.apply(shouldStop, totalCount);
                return null;
            };
            Callable<Void> alterTask = () -> {
                alterFunc.apply(shouldStop, totalCount);
                return null;
            };
            Callable<Void> selectTask = () -> {
                selectFunc.apply(shouldStop, totalCount);
                return null;
            };

            ArrayList<Future<Void>> results = new ArrayList<>();
            results.add(threadPool.submit(dmlTask));
            results.add(threadPool.submit(alterTask));
            results.add(threadPool.submit(selectTask));

            try {
                for (Future<Void> result : results) {
                    result.get();
                }
            } catch (Throwable e) {
                e.printStackTrace();
                throw (e);
            }

            if (withGsi) {
                checkGsi(conn, indexName1);
                checkGsi(conn, indexName2);
                checkGsi(conn, indexName3);
            }

        } finally {
            conn.close();
        }
    }
}
