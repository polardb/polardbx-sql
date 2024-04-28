package com.alibaba.polardbx.qatest.ddl.auto.movepartition;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MovePartitionCharsetTest extends DDLBaseNewDBTestCase {
    static private String DATABASE_NAME = "MovePartitionCharsetTest";

    private static final String PRIMARY_TABLE_NAME = "move_partition_primary";

    private static final String SINGLE_PK_TMPL = "create table {0}("
        + "id bigint not null auto_increment, "
        + "c1 char(50) default null, "
        + "c2 varchar(50) character set utf8 default null, "
        + "c3 text default null,"
        + "c4 binary(50),"
        + "c5 blob,"
        + "primary key(id)"
        + ") {1}";

    private static final String PARTITION_DEF = "partition by key(id) partitions 2";

    private final String primaryShardingDef;

    private static final String INSERT_TMPL = "insert into {0}(c1, c2, c3, c4, c5) values(?, ?, ?, ? ,?)";
    private static final String UPDATE_TMPL = "update {0} set c2=\"updated\" where id%2=0";
    private static final String DELETE_TMPL = "delete from {0} where id%3=0";

    private static final String SHOW_DS = "show ds where db='%s'";
    private static final String MOVE_PARTITION_COMMAND = "alter table %s move partitions %s to '%s'";

    private static final String SELECT_FROM_TABLE_DETAIL =
        "select storage_inst_id,table_group_name from information_schema.table_detail where table_schema='%s' and table_name='%s' and partition_name='%s'";

    private static final String PARTITION_GROUP = "p1";

    protected static final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    @Before
    public void before() {
        FailPoint.enable("FP_CATCHUP_TASK_SUSPEND", "true");

        doReCreateDatabase();

        JdbcUtil.executeUpdateSuccess(mysqlConnection, "DROP TABLE IF EXISTS " + PRIMARY_TABLE_NAME);

        dropTableWithGsi(PRIMARY_TABLE_NAME, null);
    }

    @After
    public void after() {
        FailPoint.disable("FP_CATCHUP_TASK_SUSPEND");
    }

    @Parameterized.Parameters(name = "{index}:primaryShardingDef={0}")
    public static List<String[]> prepareDate() {
        return ImmutableList.of(new String[] {PARTITION_DEF});
    }

    public MovePartitionCharsetTest(String primaryShardingDef) {
        this.primaryShardingDef = primaryShardingDef;
    }

    @Test
    public void movePartitionBackfillCharsetTest() throws SQLException {
        final String mysqlCreateTable = MessageFormat.format(SINGLE_PK_TMPL, PRIMARY_TABLE_NAME, "");
        final String tddlCreateTable =
            MessageFormat.format(SINGLE_PK_TMPL, PRIMARY_TABLE_NAME, primaryShardingDef);
        final String sqlInsert = MessageFormat.format(INSERT_TMPL, PRIMARY_TABLE_NAME);
        final String sqlUpdate = MessageFormat.format(UPDATE_TMPL, PRIMARY_TABLE_NAME);
        final String sqlDelete = MessageFormat.format(DELETE_TMPL, PRIMARY_TABLE_NAME);

        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreateTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, tddlCreateTable);

        final AtomicBoolean stop = new AtomicBoolean(false);
        final List<Future> inserts = new ArrayList<>();
        inserts.add(launchDmlCheckThread(sqlInsert, sqlUpdate, sqlDelete, stop, () -> 5));

        try {
            TimeUnit.SECONDS.sleep(3);
        } catch (InterruptedException e) {
            // ignore exception
        }

        System.out.println("primary table move partition.");

        JdbcUtil.executeUpdateSuccess(tddlConnection, "set @FP_CATCHUP_TASK_SUSPEND = '1000'");
        List<String> moveCommands = prepareAutoDbCommands();
        for (String moveCommand : moveCommands) {
            System.out.println("command: " + moveCommand);
            JdbcUtil.executeUpdateSuccess(tddlConnection, moveCommand);
            System.out.println("command end");
        }

        System.out.println("primary table move partition done.");

        try {
            TimeUnit.SECONDS.sleep(3);
        } catch (InterruptedException e) {
            // ignore exception
        }

        stop.set(true);

        for (Future future : inserts) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        // No check needed.
    }

    public final ExecutorService dmlPool = Executors.newFixedThreadPool(10);

    protected Future<?> launchDmlCheckThread(String sqlInsert, String sqlUpdate, String sqlDelete,
                                             AtomicBoolean stop, Supplier<Integer> generateBatchSize) {
        return dmlPool.submit(new InsertRunner(stop, (conn) -> {

            List<Map<Integer, ParameterContext>> batchParams = IntStream.range(0, generateBatchSize.get())
                .mapToObj(i -> ImmutableMap.<Integer, ParameterContext>builder()
                    .put(1, new ParameterContext(ParameterMethod.setBytes,
                        new Object[] {1, generateRandomUtf16Bytes()}))
                    .put(2, new ParameterContext(ParameterMethod.setBytes,
                        new Object[] {2, generateRandomUtf16Bytes()}))
                    .put(3, new ParameterContext(ParameterMethod.setBytes,
                        new Object[] {3, generateRandomUtf16Bytes()}))
                    .put(4, new ParameterContext(ParameterMethod.setBytes,
                        new Object[] {4, generateRandomUtf16Bytes()}))
                    .put(5, new ParameterContext(ParameterMethod.setBytes,
                        new Object[] {5, generateRandomUtf16Bytes()}))
                    .build())
                .collect(Collectors.toList());

            int inserted = 0;
            try (PreparedStatement ps = conn.prepareStatement(sqlInsert)) {
                try {
                    lock.readLock().lock();
                    batchParams.forEach(params -> {
                        try {
                            ParameterMethod.setParameters(ps, params);
                            ps.addBatch();
                        } catch (SQLException e) {
                            throw GeneralUtil.nestedException(e);
                        }
                    });
                    int[] result = ps.executeBatch();
                    inserted = Optional.ofNullable(result).map(r -> Arrays.stream(r).map(v -> v / -2).sum()).orElse(0);
                } finally {
                    lock.readLock().unlock();
                }
            } catch (Exception e) {
                if (!e.getMessage().contains("Deadlock found when trying to get lock") &&
                    !e.getMessage().contains("Lock wait timeout exceeded") &&
                    !e.getMessage().contains("Incorrect string value")) {
                    throw GeneralUtil.nestedException(e);
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignore) {
                }
            }

            try (Statement stmt = conn.createStatement()) {
                try {
                    lock.readLock().lock();
                    stmt.execute(sqlUpdate);
                } finally {
                    lock.readLock().unlock();
                }
            } catch (Exception e) {
                if (!e.getMessage().contains("Deadlock found when trying to get lock") &&
                    !e.getMessage().contains("Lock wait timeout exceeded")) {
                    throw GeneralUtil.nestedException(e);
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignore) {
                }
            }

            try (Statement stmt = conn.createStatement()) {
                try {
                    lock.readLock().lock();
                    stmt.execute(sqlDelete);
                } finally {
                    lock.readLock().unlock();
                }
            } catch (Exception e) {
                if (!e.getMessage().contains("Deadlock found when trying to get lock") &&
                    !e.getMessage().contains("Lock wait timeout exceeded")) {
                    throw GeneralUtil.nestedException(e);
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignore) {
                }
            }

            System.out.println(Thread.currentThread().getName() + " run DML and check.");

            return inserted;
        }, throwException, 100));
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    public static String generateRandomUTF16String(int length) {
        StringBuilder sb = new StringBuilder();
        Random random = new Random();
        for (int i = 0; i < length; i++) {
            int codePoint = random.nextInt(Character.MAX_CODE_POINT + 1);
            if (Character.isDefined(codePoint) && !Character.isSurrogate((char) codePoint)) {
                sb.appendCodePoint(codePoint);
            } else {
                i--;
            }
        }
        return sb.toString();
    }

    public static byte[] generateRandomUtf16Bytes() {
        int length = 5;  // 要生成的字符串长度
        String randomString = generateRandomUTF16String(length);

        return randomString.getBytes(StandardCharsets.UTF_16);
    }

    public List<String> prepareAutoDbCommands() throws SQLException {
        List<String> commands = new ArrayList<>();
        Set<String> instIds = new HashSet<>();
        String curInstId = null;

        String TABLE_NAME = PRIMARY_TABLE_NAME;

        String detailSql = SELECT_FROM_TABLE_DETAIL;
        String sql = String.format(detailSql, DATABASE_NAME, TABLE_NAME, PARTITION_GROUP);
        ResultSet rs = JdbcUtil.executeQuery(sql, tddlConnection);

        if (rs.next()) {
            curInstId = rs.getString("STORAGE_INST_ID");
        } else {
            throw new RuntimeException(
                String.format("not find database table %s.%s", DATABASE_NAME, TABLE_NAME));
        }
        rs.close();

        sql = String.format(SHOW_DS, DATABASE_NAME);
        rs = JdbcUtil.executeQuery(sql, tddlConnection);
        while (rs.next()) {
            if (!curInstId.equalsIgnoreCase(rs.getString("STORAGE_INST_ID"))) {
                instIds.add(rs.getString("STORAGE_INST_ID"));
            }
        }
        rs.close();

        if (!instIds.isEmpty()) {
            // move partition p3
            commands.add(
                String.format(MOVE_PARTITION_COMMAND, TABLE_NAME, PARTITION_GROUP, instIds.iterator().next()));
            commands.add(String.format(MOVE_PARTITION_COMMAND, TABLE_NAME, PARTITION_GROUP, curInstId));
        }
        return commands;
    }

    protected static final Consumer<Exception> throwException = (e) -> {
        throw GeneralUtil.nestedException(e);
    };

    protected class InsertRunner implements Runnable {

        private final AtomicBoolean stop;
        private final Function<Connection, Integer> call;
        private final Consumer<Exception> errHandler;
        private final int maxSeconds;

        public InsertRunner(AtomicBoolean stop, Function<Connection, Integer> call) {
            this(stop, call, throwException, 10); // Default insert for 10s.
        }

        public InsertRunner(AtomicBoolean stop, Function<Connection, Integer> call, Consumer<Exception> errHandler) {
            this(stop, call, errHandler, 10);
        }

        public InsertRunner(AtomicBoolean stop, Function<Connection, Integer> call, Consumer<Exception> errHandler,
                            int maxSeconds) {
            this.stop = stop;
            this.call = call;
            this.errHandler = errHandler;
            this.maxSeconds = maxSeconds;
        }

        @Override
        public void run() {
            final long startTime = System.currentTimeMillis();
            int count = 0;
            do {
                try (Connection conn = ConnectionManager.getInstance().newPolarDBXConnection()) {
                    JdbcUtil.useDb(conn, DATABASE_NAME);
                    count += call.apply(conn);
                } catch (Exception e) {
                    errHandler.accept(e);
                }

                if (System.currentTimeMillis() - startTime > maxSeconds * 1000) {
                    break; // 10s timeout, because we check after create GSI(which makes create GSI far more slower.).
                }
            } while (!stop.get());

            System.out.println(Thread.currentThread().getName() + " quit after " + count + " records inserted");
        }
    }

    void doReCreateDatabase() {
        doClearDatabase();
        String tddlSql = "use information_schema";
        JdbcUtil.executeUpdate(tddlConnection, tddlSql);
        tddlSql = "create database " + DATABASE_NAME + " partition_mode = 'auto'";
        JdbcUtil.executeUpdate(tddlConnection, tddlSql);
        tddlSql = "use " + DATABASE_NAME;
        JdbcUtil.executeUpdate(tddlConnection, tddlSql);
        JdbcUtil.executeUpdate(tddlConnection, "set sql_mode = ''");
    }

    void doClearDatabase() {
        JdbcUtil.executeUpdate(getTddlConnection1(), "use information_schema");
        String tddlSql = "drop database if exists " + DATABASE_NAME;
        JdbcUtil.executeUpdate(getTddlConnection1(), tddlSql);
    }
}
