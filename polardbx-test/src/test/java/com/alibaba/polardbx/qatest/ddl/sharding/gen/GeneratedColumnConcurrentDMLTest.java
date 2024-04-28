package com.alibaba.polardbx.qatest.ddl.sharding.gen;

import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
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

import static org.junit.Assert.assertTrue;

public class GeneratedColumnConcurrentDMLTest extends DDLBaseNewDBTestCase {
    private static final int FILL_COUNT = 1200;
    private static final int FILL_BATCH_SIZE = 1200;

    // Use logical execution since result may be different from pushdown execution
    private static final String USE_LOGICAL_EXECUTION = "DML_EXECUTION_STRATEGY=LOGICAL";
    private static final String DISABLE_DML_RETURNING = "DML_USE_RETURNING=FALSE";

    private final boolean isRDS80 = StorageInfoManager.checkRDS80(ConnectionManager.getInstance().getMysqlDataSource());

    private static String buildCmdExtra(String... params) {
        if (0 == params.length) {
            return "";
        }
        return "/*+TDDL:CMD_EXTRA(" + String.join(",", params) + ")*/";
    }

    @Test
    public void genColWithInsert() throws Exception {
        final ExecutorService threadPool = Executors.newFixedThreadPool(2);
        final List<Callable<Void>> tasks = new ArrayList<>();

        String alterSql1NotNull =
            "alter table %s add column c int not null as (a-b) logical first, add column d int not null as (a+b) logical unique first";
        String alterSql1Null =
            "alter table %s add column c int as (a-b) logical first, add column d int as (a+b) logical unique first";
        String alterSql2 = "alter table %s drop column c, drop column d";

        String alterSql1Gsi = "alter table %s add column c int as (a-b) logical first";
        String alterSql2Gsi = "alter table %s drop column c";

        String selectSql = "select * from %s";

        BiFunction<BiFunction<Integer, Integer, Boolean>, Integer[], Boolean> checker = (taskChecker, row) -> {
            if (row.length == 2) {
                // a,b
                return taskChecker.apply(row[0], row[1]);
            } else if (row.length == 4) {
                // d,c,a,b
                return taskChecker.apply(row[2], row[3]) && ((row[3] == null && row[0] == null && row[1] == null) || (
                    row[0] == row[2] + row[3] && row[1] == row[2] - row[3]));
            } else {
                return false;
            }
        };

        BiFunction<BiFunction<Integer, Integer, Boolean>, Integer[], Boolean> checkerGsi = (taskChecker, row) -> {
            if (row.length == 2) {
                // a,b
                return taskChecker.apply(row[0], row[1]);
            } else if (row.length == 3) {
                // c,a,b
                return taskChecker.apply(row[1], row[2]) && ((row[0] == null && row[2] == null) || (row[0]
                    == row[1] - row[2]));
            } else {
                return false;
            }
        };

        tasks.add(() -> {
            String tableName = "gen_col_with_insert_1";
            String colDef = "int";
            Function<Integer, String> generator =
                (count) -> String.format("insert into %%s(a,b) values (%d,%d+1)", count, count);
            BiFunction<Integer, Integer, Boolean> taskChecker = (colA, colB) -> colA + 1 == colB;
            concurrentTestInternal(tableName, colDef, alterSql1Gsi, alterSql2Gsi, selectSql, generator, generator,
                (row) -> checkerGsi.apply(taskChecker, row), false, true, 1);
            concurrentTestInternal(tableName, colDef, alterSql1NotNull, alterSql2, selectSql, generator, generator,
                (row) -> checker.apply(taskChecker, row), false, false, 1);
            System.out.println("genColWithInsert 1");
            return null;
        });

        tasks.add(() -> {
            String tableName = "gen_col_with_insert_2";
            String colDef = "int default null";
            Function<Integer, String> generator = (count) -> String.format("insert into %%s(a) values (%d)", count);
            BiFunction<Integer, Integer, Boolean> taskChecker = (colA, colB) -> colB == null;
            concurrentTestInternal(tableName, colDef, alterSql1Gsi, alterSql2Gsi, selectSql, generator, generator,
                (row) -> checkerGsi.apply(taskChecker, row), false, true, 1);
            concurrentTestInternal(tableName, colDef, alterSql1Null, alterSql2, selectSql, generator, generator,
                (row) -> checker.apply(taskChecker, row), false, false, 1);
            System.out.println("genColWithInsert 2");
            return null;
        });

        tasks.add(() -> {
            String tableName = "gen_col_with_insert_3";
            String colDef = "int default 3";
            Function<Integer, String> generator = (count) -> String.format("insert into %%s(a) values (%d)", count);
            BiFunction<Integer, Integer, Boolean> taskChecker = (colA, colB) -> colB == 3;
            concurrentTestInternal(tableName, colDef, alterSql1Gsi, alterSql2Gsi, selectSql, generator, generator,
                (row) -> checkerGsi.apply(taskChecker, row), false, true, 1);
            concurrentTestInternal(tableName, colDef, alterSql1NotNull, alterSql2, selectSql, generator, generator,
                (row) -> checker.apply(taskChecker, row), false, false, 1);
            System.out.println("genColWithInsert 3");
            return null;
        });

        tasks.add(() -> {
            String tableName = "gen_col_with_insert_4";
            String colDef = "int default 3";
            Function<Integer, String> generator =
                (count) -> String.format("insert into %%s(b,a) values (%d,%d),(%d,%d)", count * 2, count * 2 + 1,
                    count * 2 + 1, count * 2 + 2);
            BiFunction<Integer, Integer, Boolean> taskChecker = (colA, colB) -> colA - 1 == colB;
            concurrentTestInternal(tableName, colDef, alterSql1Gsi, alterSql2Gsi, selectSql, generator, generator,
                (row) -> checkerGsi.apply(taskChecker, row), false, true, 1);
            concurrentTestInternal(tableName, colDef, alterSql1NotNull, alterSql2, selectSql, generator, generator,
                (row) -> checker.apply(taskChecker, row), false, false, 1);
            System.out.println("genColWithInsert 4");
            return null;
        });

        tasks.add(() -> {
            String tableName = "gen_col_with_insert_5";
            String colDef = "int default 3";
            Function<Integer, String> generator =
                (count) -> String.format("insert into %%s(b,a) values (%d*2,%d*2+1),(%d*2+1,%d*2+2)", count, count,
                    count, count);
            BiFunction<Integer, Integer, Boolean> taskChecker = (colA, colB) -> colA - 1 == colB;
            concurrentTestInternal(tableName, colDef, alterSql1Gsi, alterSql2Gsi, selectSql, generator, generator,
                (row) -> checkerGsi.apply(taskChecker, row), false, true, 1);
            concurrentTestInternal(tableName, colDef, alterSql1NotNull, alterSql2, selectSql, generator, generator,
                (row) -> checker.apply(taskChecker, row), false, false, 1);
            System.out.println("genColWithInsert 5");
            return null;
        });

        ArrayList<Future<Void>> results = new ArrayList<>();
        for (Callable<Void> task : tasks) {
            results.add(threadPool.submit(task));
        }

        try {
            for (Future<Void> result : results) {
                result.get();
            }
        } finally {
            threadPool.shutdown();
        }
    }

    @Test
    public void genColWithInsertSelect() throws Exception {
        final ExecutorService threadPool = Executors.newFixedThreadPool(2);
        final List<Callable<Void>> tasks = new ArrayList<>();

        String selectTableName = "gen_col_insert_select_src_tbl";
        buildSelectTable(selectTableName);

        String alterSql1NotNull =
            "alter table %s add column c int not null as (a-b) logical first, add column d int not null as (a+b) logical unique first";
        String alterSql1Null =
            "alter table %s add column c int as (a-b) logical first, add column d int as (a+b) logical unique first";
        String alterSql2 = "alter table %s drop column c, drop column d";

        String alterSql1Gsi = "alter table %s add column c int as (a-b) logical first";
        String alterSql2Gsi = "alter table %s drop column c";

        String selectSql = "select * from %s";

        BiFunction<BiFunction<Integer, Integer, Boolean>, Integer[], Boolean> checker = (taskChecker, row) -> {
            if (row.length == 2) {
                // a,b
                return taskChecker.apply(row[0], row[1]);
            } else if (row.length == 4) {
                // d,c,a,b
                return taskChecker.apply(row[2], row[3]) && ((row[3] == null && row[0] == null && row[1] == null) || (
                    row[0] == row[2] + row[3] && row[1] == row[2] - row[3]));
            } else {
                return false;
            }
        };

        BiFunction<BiFunction<Integer, Integer, Boolean>, Integer[], Boolean> checkerGsi = (taskChecker, row) -> {
            if (row.length == 2) {
                // a,b
                return taskChecker.apply(row[0], row[1]);
            } else if (row.length == 3) {
                // c,a,b
                return taskChecker.apply(row[1], row[2]) && ((row[0] == null && row[2] == null) || (row[0]
                    == row[1] - row[2]));
            } else {
                return false;
            }
        };

        tasks.add(() -> {
            String tableName = "gen_col_with_insert_select_1";
            String colDef = "int";
            Function<Integer, String> generator =
                (count) -> String.format("insert into %%s(a,b) select %d,%d+1", count, count);
            BiFunction<Integer, Integer, Boolean> taskChecker = (colA, colB) -> colA + 1 == colB;
            concurrentTestInternal(tableName, colDef, alterSql1Gsi, alterSql2Gsi, selectSql, generator, generator,
                (row) -> checkerGsi.apply(taskChecker, row), false, true, 1);
            concurrentTestInternal(tableName, colDef, alterSql1NotNull, alterSql2, selectSql, generator, generator,
                (row) -> checker.apply(taskChecker, row), false, false, 1);
            System.out.println("genColWithInsertSelect 1");
            return null;
        });

        tasks.add(() -> {
            String tableName = "gen_col_with_insert_select_2";
            String colDef = "int";
            Function<Integer, String> generator =
                (count) -> String.format("insert into %%s(b,a) select c,d+1 from %s where c=%d", selectTableName,
                    count);
            BiFunction<Integer, Integer, Boolean> taskChecker = (colA, colB) -> colA == colB + 1;
            concurrentTestInternal(tableName, colDef, alterSql1Gsi, alterSql2Gsi, selectSql, generator, generator,
                (row) -> checkerGsi.apply(taskChecker, row), false, true, 1);
            concurrentTestInternal(tableName, colDef, alterSql1NotNull, alterSql2, selectSql, generator, generator,
                (row) -> checker.apply(taskChecker, row), false, false, 1);
            System.out.println("genColWithInsertSelect 2");
            return null;
        });

        tasks.add(() -> {
            String tableName = "gen_col_with_insert_select_3";
            String colDef = "int default 3";
            Function<Integer, String> generator =
                (count) -> String.format("insert into %%s(a) select c from %s where c=%d", selectTableName, count);
            BiFunction<Integer, Integer, Boolean> taskChecker = (colA, colB) -> colB == 3;
            concurrentTestInternal(tableName, colDef, alterSql1Gsi, alterSql2Gsi, selectSql, generator, generator,
                (row) -> checkerGsi.apply(taskChecker, row), false, true, 1);
            concurrentTestInternal(tableName, colDef, alterSql1Null, alterSql2, selectSql, generator, generator,
                (row) -> checker.apply(taskChecker, row), false, false, 1);
            System.out.println("genColWithInsertSelect 3");
            return null;
        });

        ArrayList<Future<Void>> results = new ArrayList<>();
        for (Callable<Void> task : tasks) {
            results.add(threadPool.submit(task));
        }

        try {
            for (Future<Void> result : results) {
                result.get();
            }
        } finally {
            threadPool.shutdown();
        }
    }

    @Test
    public void genColWithUpdate() throws Exception {
        final ExecutorService threadPool = Executors.newFixedThreadPool(2);
        final List<Callable<Void>> tasks = new ArrayList<>();

        String alterSql1NotNull =
            "alter table %s add column c int not null as (a-b) logical first, add column d int as (a+b) logical first";
        String alterSql1Null =
            "alter table %s add column c int as (a-b) logical first, add column d int as (a+b) logical first";
        String alterSql2 = "alter table %s drop column c, drop column d";

        String alterSql1Gsi = "alter table %s add column c int as (a-b) logical first";
        String alterSql2Gsi = "alter table %s drop column c";

        String selectSql = "select * from %s order by a";

        BiFunction<BiFunction<Integer, Integer, Boolean>, Integer[], Boolean> checker = (taskChecker, row) -> {
            if (row.length == 2) {
                // a,b
                return taskChecker.apply(row[0], row[1]);
            } else if (row.length == 4) {
                // d,c,a,b
                return taskChecker.apply(row[2], row[3]) && ((row[3] == null && row[0] == null && row[1] == null) || (
                    row[0] == row[2] + row[3] && row[1] == row[2] - row[3]));
            } else {
                return false;
            }
        };

        BiFunction<BiFunction<Integer, Integer, Boolean>, Integer[], Boolean> checkerGsi = (taskChecker, row) -> {
            if (row.length == 2) {
                // a,b
                return taskChecker.apply(row[0], row[1]);
            } else if (row.length == 3) {
                // c,a,b
                return taskChecker.apply(row[1], row[2]) && ((row[0] == null && row[2] == null) || (row[0]
                    == row[1] - row[2]));
            } else {
                return false;
            }
        };

        tasks.add(() -> {
            String tableName = "gen_col_with_update_1";
            String colDef = "int";
            Function<Integer, String> generator =
                (count) -> String.format("update %%s set b=%d+1 where a=%d", count, count);
            BiFunction<Integer, Integer, Boolean> taskChecker = (colA, colB) -> colA + 1 == colB;
            concurrentTestInternal(tableName, colDef, alterSql1Gsi, alterSql2Gsi, selectSql, generator, generator,
                (row) -> checkerGsi.apply(taskChecker, row), true, true, 1);
            concurrentTestInternal(tableName, colDef, alterSql1NotNull, alterSql2, selectSql, generator, generator,
                (row) -> checker.apply(taskChecker, row), true, false, 1);
            System.out.println("genColWithUpdate 1");
            return null;
        });

        tasks.add(() -> {
            String tableName = "gen_col_with_update_2";
            String colDef = "int";
            Function<Integer, String> generator =
                (count) -> String.format("update %%s set a=%d-1 where b=%d", count, count);
            BiFunction<Integer, Integer, Boolean> taskChecker = (colA, colB) -> colA + 1 == colB;
            concurrentTestInternal(tableName, colDef, alterSql1Gsi, alterSql2Gsi, selectSql, generator, generator,
                (row) -> checkerGsi.apply(taskChecker, row), true, true, 1);
            concurrentTestInternal(tableName, colDef, alterSql1NotNull, alterSql2, selectSql, generator, generator,
                (row) -> checker.apply(taskChecker, row), true, false, 1);
            System.out.println("genColWithUpdate 2");
            return null;
        });

        tasks.add(() -> {
            String tableName = "gen_col_with_update_3";
            String colDef = "int";
            Function<Integer, String> generator =
                (count) -> String.format("update %%s set a=%d-1 where b=%d", count, count);
            BiFunction<Integer, Integer, Boolean> taskChecker = (colA, colB) -> colA + 1 == colB;
            concurrentTestInternal(tableName, colDef, alterSql1Gsi, alterSql2Gsi, selectSql, generator, generator,
                (row) -> checkerGsi.apply(taskChecker, row), true, true, 1);
            concurrentTestInternal(tableName, colDef, alterSql1NotNull, alterSql2, selectSql, generator, generator,
                (row) -> checker.apply(taskChecker, row), true, false, 1);
            System.out.println("genColWithUpdate 3");
            return null;
        });

        tasks.add(() -> {
            String tableName = "gen_col_with_update_4";
            String colDef = "int default 4";
            Function<Integer, String> generator =
                (count) -> String.format("update %%s set b=default where a=%d", count);
            BiFunction<Integer, Integer, Boolean> taskChecker = (colA, colB) -> colB == 4;
            concurrentTestInternal(tableName, colDef, alterSql1Gsi, alterSql2Gsi, selectSql, generator, generator,
                (row) -> checkerGsi.apply(taskChecker, row), true, true, 1);
            concurrentTestInternal(tableName, colDef, alterSql1NotNull, alterSql2, selectSql, generator, generator,
                (row) -> checker.apply(taskChecker, row), true, false, 1);
            System.out.println("genColWithUpdate 4");
            return null;
        });

        tasks.add(() -> {
            String tableName = "gen_col_with_update_5";
            String colDef = "int";
            Function<Integer, String> generator =
                (count) -> String.format("update %%s set b=default(b) where a=%d", count);
            BiFunction<Integer, Integer, Boolean> taskChecker = (colA, colB) -> colB == null;
            concurrentTestInternal(tableName, colDef, alterSql1Gsi, alterSql2Gsi, selectSql, generator, generator,
                (row) -> checkerGsi.apply(taskChecker, row), true, true, 1);
            concurrentTestInternal(tableName, colDef, alterSql1Null, alterSql2, selectSql, generator, generator,
                (row) -> checker.apply(taskChecker, row), true, false, 1);
            System.out.println("genColWithUpdate 5");
            return null;
        });

        tasks.add(() -> {
            String tableName = "gen_col_with_update_6";
            String colDef = "int";
            Function<Integer, String> generator =
                (count) -> String.format("update %%s set b=a+1,a=%d,b=a+1,b=b+1 where a=%d", count, count);
            BiFunction<Integer, Integer, Boolean> taskChecker = (colA, colB) -> colB == colA + 2;
            concurrentTestInternal(tableName, colDef, alterSql1Gsi, alterSql2Gsi, selectSql, generator, generator,
                (row) -> checkerGsi.apply(taskChecker, row), true, true, 1);
            concurrentTestInternal(tableName, colDef, alterSql1NotNull, alterSql2, selectSql, generator, generator,
                (row) -> checker.apply(taskChecker, row), true, false, 1);
            System.out.println("genColWithUpdate 6");
            return null;
        });

        ArrayList<Future<Void>> results = new ArrayList<>();
        for (Callable<Void> task : tasks) {
            results.add(threadPool.submit(task));
        }

        try {
            for (Future<Void> result : results) {
                result.get();
            }
        } finally {
            threadPool.shutdown();
        }
    }

    @Test
    public void genColWithReplace() throws Exception {
        final ExecutorService threadPool = Executors.newFixedThreadPool(2);
        final List<Callable<Void>> tasks = new ArrayList<>();

        String hint = buildCmdExtra(DISABLE_DML_RETURNING, USE_LOGICAL_EXECUTION);

        String alterSql1 =
            "alter table %s add column c int as (a-b) logical first, add column d int as (a+b) logical first";
        String alterSql2 = "alter table %s drop column c, drop column d";

        String alterSql1Gsi = "alter table %s add column c int as (a-b) logical first";
        String alterSql2Gsi = "alter table %s drop column c";

        BiFunction<BiFunction<Integer, Integer, Boolean>, Integer[], Boolean> checker = (taskChecker, row) -> {
            if (row.length == 2) {
                // a,b
                return taskChecker.apply(row[0], row[1]);
            } else if (row.length == 4) {
                // d,c,a,b
                return taskChecker.apply(row[2], row[3]) && ((row[3] == null && row[0] == null && row[1] == null) || (
                    row[0] == row[2] + row[3] && row[1] == row[2] - row[3]));
            } else {
                return false;
            }
        };

        BiFunction<BiFunction<Integer, Integer, Boolean>, Integer[], Boolean> checkerGsi = (taskChecker, row) -> {
            if (row.length == 2) {
                // a,b
                return taskChecker.apply(row[0], row[1]);
            } else if (row.length == 3) {
                // c,a,b
                return taskChecker.apply(row[1], row[2]) && ((row[0] == null && row[2] == null) || (row[0]
                    == row[1] - row[2]));
            } else {
                return false;
            }
        };

        tasks.add(() -> {
            String tableName = "gen_col_with_replace_1";
            String colDef = "int";
            String selectSql = "select * from %s order by a";
            Function<Integer, String> generator =
                (count) -> hint + String.format("replace into %%s(a,b) values (%d, %d+%d)", count, count, FILL_COUNT);
            BiFunction<Integer, Integer, Boolean> taskChecker = (colA, colB) -> colA + FILL_COUNT == colB;
            concurrentTestInternal(tableName, colDef, alterSql1Gsi, alterSql2Gsi, selectSql, generator, generator,
                (row) -> checkerGsi.apply(taskChecker, row), true, true, 1);
            concurrentTestInternal(tableName, colDef, alterSql1, alterSql2, selectSql, generator, generator,
                (row) -> checker.apply(taskChecker, row), true, false, 1);
            System.out.println("genColWithReplace 1");
            return null;
        });

        tasks.add(() -> {
            String tableName = "gen_col_with_replace_2";
            String colDef = "int unique key";
            String selectSql = "select * from %s order by a desc";
            Function<Integer, String> generator =
                (count) -> hint + String.format("replace into %%s(a,b) values (%d+%d, %d)", count, FILL_COUNT, count);
            BiFunction<Integer, Integer, Boolean> taskChecker = (colA, colB) -> colA - FILL_COUNT == colB;
            concurrentTestInternal(tableName, colDef, alterSql1Gsi, alterSql2Gsi, selectSql, generator, generator,
                (row) -> checkerGsi.apply(taskChecker, row), true, true, 1);
            concurrentTestInternal(tableName, colDef, alterSql1, alterSql2, selectSql, generator, generator,
                (row) -> checker.apply(taskChecker, row), true, false, 1);
            System.out.println("genColWithReplace 2");
            return null;
        });

        ArrayList<Future<Void>> results = new ArrayList<>();
        for (Callable<Void> task : tasks) {
            results.add(threadPool.submit(task));
        }

        try {
            for (Future<Void> result : results) {
                result.get();
            }
        } finally {
            threadPool.shutdown();
        }
    }

    @Test
    public void genColWithUpsert() throws Exception {
        final ExecutorService threadPool = Executors.newFixedThreadPool(2);
        final List<Callable<Void>> tasks = new ArrayList<>();

        String hint = buildCmdExtra(DISABLE_DML_RETURNING, USE_LOGICAL_EXECUTION);

        String alterSql1 =
            "alter table %s add column c int not null as (a-b) logical first, add column d int as (a+b) logical first";
        String alterSql2 = "alter table %s drop column c, drop column d";

        String alterSql1Gsi = "alter table %s add column c int as (a-b) logical first";
        String alterSql2Gsi = "alter table %s drop column c";

        BiFunction<BiFunction<Integer, Integer, Boolean>, Integer[], Boolean> checker = (taskChecker, row) -> {
            if (row.length == 2) {
                // a,b
                return taskChecker.apply(row[0], row[1]);
            } else if (row.length == 4) {
                // d,c,a,b
                return taskChecker.apply(row[2], row[3]) && ((row[3] == null && row[0] == null && row[1] == null) || (
                    row[0] == row[2] + row[3] && row[1] == row[2] - row[3]));
            } else {
                return false;
            }
        };

        BiFunction<BiFunction<Integer, Integer, Boolean>, Integer[], Boolean> checkerGsi = (taskChecker, row) -> {
            if (row.length == 2) {
                // a,b
                return taskChecker.apply(row[0], row[1]);
            } else if (row.length == 3) {
                // c,a,b
                return taskChecker.apply(row[1], row[2]) && ((row[0] == null && row[2] == null) || (row[0]
                    == row[1] - row[2]));
            } else {
                return false;
            }
        };

        tasks.add(() -> {
            String tableName = "gen_col_with_upsert_1";
            String colDef = "int";
            String selectSql = "select * from %s order by a";
            Function<Integer, String> generator =
                (count) -> hint + String.format("insert into %%s(a,b) values (%d, %d) on duplicate key update b=b+%d",
                    count, count, FILL_COUNT);
            BiFunction<Integer, Integer, Boolean> taskChecker = (colA, colB) -> colA + FILL_COUNT == colB;
            concurrentTestInternal(tableName, colDef, alterSql1Gsi, alterSql2Gsi, selectSql, generator, generator,
                (row) -> checkerGsi.apply(taskChecker, row), true, true, 1);
            concurrentTestInternal(tableName, colDef, alterSql1, alterSql2, selectSql, generator, generator,
                (row) -> checker.apply(taskChecker, row), true, false, 1);
            System.out.println("genColWithUpsert 1");
            return null;
        });

        tasks.add(() -> {
            String tableName = "gen_col_with_upsert_2";
            String colDef = "int";
            String selectSql = "select * from %s order by a desc";
            Function<Integer, String> generator =
                (count) -> hint + String.format("insert into %%s(a,b) values (%d, %d) on duplicate key update a=a+%d",
                    count, count, FILL_COUNT);
            BiFunction<Integer, Integer, Boolean> taskChecker = (colA, colB) -> colA - FILL_COUNT == colB;
            concurrentTestInternal(tableName, colDef, alterSql1Gsi, alterSql2Gsi, selectSql, generator, generator,
                (row) -> checkerGsi.apply(taskChecker, row), true, true, 1);
            concurrentTestInternal(tableName, colDef, alterSql1, alterSql2, selectSql, generator, generator,
                (row) -> checker.apply(taskChecker, row), true, false, 1);
            System.out.println("genColWithUpsert 2");
            return null;
        });

        tasks.add(() -> {
            String tableName = "gen_col_with_upsert_3";
            String colDef = "int unique key";
            String selectSql = "select * from %s order by a";
            Function<Integer, String> generator = (count) -> hint + String.format(
                "insert into %%s(a,b) values (%d, %d+%d) on duplicate key update b=b+%d", count, count, FILL_COUNT,
                FILL_COUNT);
            BiFunction<Integer, Integer, Boolean> taskChecker = (colA, colB) -> colA + FILL_COUNT == colB;
            concurrentTestInternal(tableName, colDef, alterSql1Gsi, alterSql2Gsi, selectSql, generator, generator,
                (row) -> checkerGsi.apply(taskChecker, row), true, true, 1);
            concurrentTestInternal(tableName, colDef, alterSql1, alterSql2, selectSql, generator, generator,
                (row) -> checker.apply(taskChecker, row), true, false, 1);
            System.out.println("genColWithUpsert 3");
            return null;
        });

        ArrayList<Future<Void>> results = new ArrayList<>();
        for (Callable<Void> task : tasks) {
            results.add(threadPool.submit(task));
        }

        try {
            for (Future<Void> result : results) {
                result.get();
            }
        } finally {
            threadPool.shutdown();
        }
    }

    @Test
    public void genColWithInsertIgnore() throws Exception {
        final ExecutorService threadPool = Executors.newFixedThreadPool(2);
        final List<Callable<Void>> tasks = new ArrayList<>();

        String hint = buildCmdExtra(DISABLE_DML_RETURNING, USE_LOGICAL_EXECUTION);

        String alterSql1 =
            "alter table %s add column c int not null as (a-b) logical first, add column d int as (a+b) logical first";
        String alterSql2 = "alter table %s drop column c, drop column d";

        String alterSql1Gsi = "alter table %s add column c int as (a-b) logical first";
        String alterSql2Gsi = "alter table %s drop column c";

        BiFunction<BiFunction<Integer, Integer, Boolean>, Integer[], Boolean> checker = (taskChecker, row) -> {
            if (row.length == 2) {
                // a,b
                return taskChecker.apply(row[0], row[1]);
            } else if (row.length == 4) {
                // d,c,a,b
                return taskChecker.apply(row[2], row[3]) && ((row[3] == null && row[0] == null && row[1] == null) || (
                    row[0] == row[2] + row[3] && row[1] == row[2] - row[3]));
            } else {
                return false;
            }
        };

        BiFunction<BiFunction<Integer, Integer, Boolean>, Integer[], Boolean> checkerGsi = (taskChecker, row) -> {
            if (row.length == 2) {
                // a,b
                return taskChecker.apply(row[0], row[1]);
            } else if (row.length == 3) {
                // c,a,b
                return taskChecker.apply(row[1], row[2]) && ((row[0] == null && row[2] == null) || (row[0]
                    == row[1] - row[2]));
            } else {
                return false;
            }
        };

        tasks.add(() -> {
            String tableName = "gen_col_with_insert_ignore_1";
            String colDef = "int";
            String selectSql = "select * from %s order by a";
            Function<Integer, String> generator =
                (count) -> hint + String.format("insert ignore into %%s(a,b) values (%d, %d+%d)", count, count,
                    FILL_COUNT);
            BiFunction<Integer, Integer, Boolean> taskChecker = Objects::equals;
            concurrentTestInternal(tableName, colDef, alterSql1Gsi, alterSql2Gsi, selectSql, generator, generator,
                (row) -> checkerGsi.apply(taskChecker, row), true, true, 1);
            concurrentTestInternal(tableName, colDef, alterSql1, alterSql2, selectSql, generator, generator,
                (row) -> checker.apply(taskChecker, row), true, false, 1);
            System.out.println("genColWithInsertIgnore 1");
            return null;
        });

        tasks.add(() -> {
            String tableName = "gen_col_with_insert_ignore_2";
            String colDef = "int unique key";
            String selectSql = "select * from %s order by a desc";
            Function<Integer, String> generator =
                (count) -> hint + String.format("insert ignore into %%s(a,b) values (%d+%d, %d)", count, FILL_COUNT,
                    count);
            BiFunction<Integer, Integer, Boolean> taskChecker = Objects::equals;
            concurrentTestInternal(tableName, colDef, alterSql1Gsi, alterSql2Gsi, selectSql, generator, generator,
                (row) -> checkerGsi.apply(taskChecker, row), true, true, 1);
            concurrentTestInternal(tableName, colDef, alterSql1, alterSql2, selectSql, generator, generator,
                (row) -> checker.apply(taskChecker, row), true, false, 1);
            System.out.println("genColWithInsertIgnore 2");
            return null;
        });

        ArrayList<Future<Void>> results = new ArrayList<>();
        for (Callable<Void> task : tasks) {
            results.add(threadPool.submit(task));
        }

        try {
            for (Future<Void> result : results) {
                result.get();
            }
        } finally {
            threadPool.shutdown();
        }
    }

    public void buildSelectTable(String selectTableName) {
        dropTableIfExists(selectTableName);
        String createSql =
            String.format("create table %s (" + "c int primary key, " + "d int " + ") dbpartition by hash(`c`)",
                selectTableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, createSql);

        for (int i = 0; i < FILL_COUNT; i++) {
            String sql = String.format("insert into table %s values (%d, %d)", selectTableName, i, i);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        }
    }

    public void concurrentTestInternal(String tableName, String colDef, String alterSql1, String alterSql2,
                                       String selectSql, Function<Integer, String> generator1,
                                       Function<Integer, String> generator2, Function<Integer[], Boolean> checker,
                                       boolean fillData, boolean withGsi, int batchSize) throws Exception {
        tableName = tableName + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        String indexName1 = tableName + "_idx_1";
        Connection conn = getPolardbxConnection();
        String finalTableName = tableName;

        try {
            String createSql = withGsi ? String.format(
                "create table %s ("
                    + "a int primary key, "
                    + "b %s, "
                    + "clustered index %s(`a`) dbpartition by hash(`a`)"
                    + ") dbpartition by hash(`a`)",
                tableName, colDef, indexName1) :
                String.format(
                    "create table %s ("
                        + "a int primary key, "
                        + "b %s "
                        + ") dbpartition by hash(`a`)",
                    tableName, colDef);

            // Retry in case of table group not exist
            int retryCnt = 0;
            while (true) {
                try {
                    JdbcUtil.executeUpdateSuccess(conn, createSql);
                    break;
                } catch (Throwable e) {
                    retryCnt++;
                    if (retryCnt > 5) {
                        throw e;
                    }
                    System.out.println("retry " + retryCnt + " " + e.getMessage());
                }
            }

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
                                    if (e.getMessage().contains("Lock wait timeout exceeded") || e.getMessage()
                                        .contains("Deadlock found")) {
                                        // ignore
                                        totalCount.getAndDecrement();
                                    } else if ((e.getMessage().contains("Unknown target column") || e.getMessage()
                                        .contains("not found")) && !changed) {
                                        changed = true;
                                        generator = generator2;
                                        totalCount.getAndDecrement();
                                    } else if (e.getMessage().contains("Lock wait timeout exceeded")) {
                                        // ignore
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
                        try {
                            Thread.sleep(1000);
                            String sql = String.format(alterSql1, finalTableName);
                            execDdlWithRetry(tddlDatabase1, finalTableName, sql, connection);
                            Thread.sleep(1000);
                            sql = String.format(alterSql2, finalTableName);
                            execDdlWithRetry(tddlDatabase1, finalTableName, sql, connection);
                            Thread.sleep(1000);
                        } finally {
                            shouldStop.set(true);
                            connection.close();
                        }
                        return null;
                    }
                };

            List<String> index = new ArrayList<>();
            index.add(" force index (primary)");
            if (withGsi) {
                index.add(String.format(" force index (%s) ", indexName1));
            }

            BiFunction<AtomicBoolean, AtomicInteger, Void> selectFunc =
                new BiFunction<AtomicBoolean, AtomicInteger, Void>() {
                    @SneakyThrows
                    @Override
                    public Void apply(AtomicBoolean shouldStop, AtomicInteger totalCount) {
                        Connection connection = ConnectionManager.getInstance().newPolarDBXConnection();
                        useDb(connection, tddlDatabase1);
                        JdbcUtil.executeUpdateSuccess(connection, "SET ENABLE_MPP=FALSE");
                        try {
                            int cnt = 0;
                            int prevColCnt = 2;
                            int changeCnt = 0;
                            Statement stmt = connection.createStatement();
                            while ((!shouldStop.get() || fillData) && totalCount.get() < FILL_COUNT) {
                                int tot = totalCount.get();
                                try (ResultSet rs = stmt.executeQuery(
                                    String.format(selectSql, finalTableName + index.get(cnt % index.size())))) {
                                    cnt++;
                                    ResultSetMetaData rsmd = rs.getMetaData();

                                    int colCnt = rsmd.getColumnCount();
                                    if (prevColCnt != colCnt) {
                                        prevColCnt = colCnt;
                                        changeCnt++;
                                    }

                                    while (rs.next() && tot > 0) {
                                        Integer[] row = new Integer[colCnt];
                                        for (int i = 0; i < colCnt; i++) {
                                            Object obj = rs.getObject(i + 1);
                                            row[i] = obj == null ? null : (Integer) obj;
                                        }
                                        if (!checker.apply(row)) {
                                            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                                            Date now = new Date();
                                            System.out.println(
                                                sdf.format(now) + " " + tot + ": " + Arrays.toString(row));
                                        }
                                        assertTrue(checker.apply(row));
                                        tot--;
                                    }
                                } catch (SQLException e) {
                                    System.out.println(e.getMessage());
                                    if (isRDS80) {
                                        if (e.getMessage().contains("Communications link failure")) {
                                            connection = getTddlJdbcConnection();
                                            stmt = connection.createStatement();
                                        } else if (e.getMessage().contains(
                                            "The definition of the table required by the flashback query has changed ")) {
                                            // ignore
                                        } else {
                                            throw (e);
                                        }
                                    }
                                }
                                Thread.sleep(100);
                            }
                            stmt.close();
                            Assert.assertTrue(changeCnt <= 2);
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
            } finally {
                //报错需设置退出信号,防止线程泄漏
                shouldStop.set(true);
                totalCount.set(FILL_COUNT);
                threadPool.shutdown();
            }

            if (withGsi) {
                checkGsi(conn, indexName1);
            }
        } finally {
            conn.close();
        }
    }

    Connection getTddlJdbcConnection() {
        Connection connection = ConnectionManager.getInstance().newPolarDBXConnection();
        useDb(connection, tddlDatabase1);
        JdbcUtil.executeUpdateSuccess(connection, "SET ENABLE_MPP=FALSE");
        return connection;
    }
}
