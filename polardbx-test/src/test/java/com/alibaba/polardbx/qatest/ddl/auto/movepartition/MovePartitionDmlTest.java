package com.alibaba.polardbx.qatest.ddl.auto.movepartition;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.ddl.sharding.gsi.group2.GsiBackfillTest;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

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
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

public class MovePartitionDmlTest extends DDLBaseNewDBTestCase {
    static private final String DATABASE_NAME = "MovePartitionDmlTest";

    private static final String PRIMARY_TABLE_NAME = "move_partition_primary";

    private static final String INDEX_NAME = "gsi_move";

    private static final String SINGLE_PK_TMPL = "create table {0}("
        + "id bigint not null auto_increment, "
        + "c1 bigint default null, "
        + "c2 varchar(256) default null, "
        + "primary key(id),"
        + "key i_c2(c1, c2)"
        + ") {1}";

    private static final String PARTITION_DEF = "partition by key(c1) partitions 2";
    private static final String PARTITION_DEF1 = "partition by key(c2) partitions 2";

    private static final String CREATE_GSI_TMPL =
        "create global index {0} on {1}(id) covering(c2) partition by key(id) partitions 2";

    private static final String INSERT_TMPL = "insert into {0}(c1, c2) values(?, ?)";
    private static final String UPDATE_TMPL = "update {0} set c2=\"updated\" where id%2=0";
    private static final String DELETE_TMPL = "delete from {0} where id%3=0";

    private static final String SELECT_IGNORE_TMPL =
        "/*+TDDL: cmd_extra(ENABLE_MPP = false)*/select id,c1,c2 from {0} ignore index({1}) order by id";

    private static final String SELECT_FORCE_TMPL =
        "/*+TDDL: cmd_extra(ENABLE_MPP = false)*/select id,c1,c2 from {0} force index({1}) order by id";

    private static final String SLOW_HINT =
        "/*+TDDL: cmd_extra(GSI_DEBUG=\"slow\")*/";

    private static final String SHOW_DS = "show ds where db='%s'";
    private static final String MOVE_PARTITION_COMMAND = "alter table %s move partitions %s to '%s'";

    private static final String SELECT_FROM_TABLE_DETAIL =
        "select storage_inst_id,table_group_name from information_schema.table_detail where table_schema='%s' and table_name='%s' and partition_name='%s'";

    private static final String SELECT_FROM_TABLE_DETAIL_GSI =
        "select storage_inst_id,table_group_name from information_schema.table_detail where table_schema='%s' and index_name='%s' and partition_name='%s'";

    private static final String PARTITION_GROUP = "p1";

    private final String primaryShardingDef;

    private static final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    @Parameterized.Parameters(name = "{index}:primaryShardingDef={0}")
    public static List<String[]> prepareDate() {
        return ImmutableList.of(new String[] {PARTITION_DEF}, new String[] {PARTITION_DEF1});
    }

    @Before
    public void before() {
        doReCreateDatabase();

        JdbcUtil.executeUpdateSuccess(mysqlConnection, "DROP TABLE IF EXISTS " + PRIMARY_TABLE_NAME);

        dropTableWithGsi(PRIMARY_TABLE_NAME, ImmutableList.of(INDEX_NAME));
    }

    public MovePartitionDmlTest(String primaryShardingDef) {
        this.primaryShardingDef = primaryShardingDef;
    }

    @Test
    public void singlePkMovePartitionDmlTest() throws SQLException {
        final String mysqlCreateTable = MessageFormat.format(SINGLE_PK_TMPL, PRIMARY_TABLE_NAME, "");
        final String tddlCreateTable =
            MessageFormat.format(SINGLE_PK_TMPL, PRIMARY_TABLE_NAME, primaryShardingDef);
        final String sqlInsert = MessageFormat.format(INSERT_TMPL, PRIMARY_TABLE_NAME);
        final String sqlUpdate = MessageFormat.format(UPDATE_TMPL, PRIMARY_TABLE_NAME);
        final String sqlDelete = MessageFormat.format(DELETE_TMPL, PRIMARY_TABLE_NAME);
        final String sqlCreateGsi = MessageFormat.format(CREATE_GSI_TMPL, INDEX_NAME, PRIMARY_TABLE_NAME);
        final String sqlSelectPrimary = MessageFormat.format(SELECT_IGNORE_TMPL, PRIMARY_TABLE_NAME, INDEX_NAME);
        final String sqlSelectGsi = MessageFormat.format(SELECT_FORCE_TMPL, PRIMARY_TABLE_NAME, INDEX_NAME);

        JdbcUtil.executeUpdateSuccess(mysqlConnection, mysqlCreateTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, tddlCreateTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sqlCreateGsi);

        final AtomicBoolean stop = new AtomicBoolean(false);
        final List<Future> inserts = new ArrayList<>();
        final AtomicLong pkGen = new AtomicLong(0);
        inserts.add(launchDmlCheckThread(sqlInsert, sqlUpdate, sqlDelete, sqlSelectPrimary, sqlSelectGsi, stop,
            () -> pkGen.getAndIncrement() % 8, () -> 5));

        try {
            TimeUnit.SECONDS.sleep(3);
        } catch (InterruptedException e) {
            // ignore exception
        }

        System.out.println("primary table move partition.");

        List<String> moveCommands = prepareAutoDbCommands(false);
        for (String moveCommand : moveCommands) {
            System.out.println("command: " + moveCommand);
            JdbcUtil.executeUpdateSuccess(tddlConnection, SLOW_HINT + moveCommand);
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

    private List<String> prepareAutoDbCommands(Boolean isGsi) throws SQLException {
        List<String> commands = new ArrayList<>();
        Set<String> instIds = new HashSet<>();
        String curInstId = null;

        String TABLE_NAME = isGsi ? INDEX_NAME : PRIMARY_TABLE_NAME;

        String detailSql = isGsi ? SELECT_FROM_TABLE_DETAIL_GSI : SELECT_FROM_TABLE_DETAIL;
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

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    private final ExecutorService dmlPool = Executors.newFixedThreadPool(10);

    protected Future<?> launchDmlCheckThread(String sqlInsert, String sqlUpdate, String sqlDelete,
                                             String sqlSelectPrimary, String sqlSelectGSI,
                                             AtomicBoolean stop, Supplier<Long> generateSk,
                                             Supplier<Integer> generateBatchSize) {
        return dmlPool.submit(new InsertRunner(stop, (conn) -> {
            // List<Pair< sql, error_message >>
            List<Pair<String, Exception>> failedList = new ArrayList<>();

            final ParameterContext skPc = Optional.ofNullable(generateSk.get())
                .map(skv -> new ParameterContext(ParameterMethod.setLong, new Object[] {1, skv}))
                .orElse(new ParameterContext(ParameterMethod.setNull1, new Object[] {1, null}));

            List<Map<Integer, ParameterContext>> batchParams = IntStream.range(0, generateBatchSize.get())
                .mapToObj(i -> ImmutableMap.<Integer, ParameterContext>builder()
                    .put(1, skPc)
                    .put(2,
                        new ParameterContext(ParameterMethod.setString,
                            new Object[] {2, RandomStringUtils.randomAlphabetic(20)}))
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
                    !e.getMessage().contains("Lock wait timeout exceeded")) {
                    throw GeneralUtil.nestedException(e);
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignore) {
                }
            }

            try {
                lock.writeLock().lock();
                selectContentSameAssert(sqlSelectPrimary, sqlSelectGSI, null, conn, conn);
            } finally {
                lock.writeLock().unlock();
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

            try {
                lock.writeLock().lock();
                selectContentSameAssert(sqlSelectPrimary, sqlSelectGSI, null, conn, conn);
            } finally {
                lock.writeLock().unlock();
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

            try {
                lock.writeLock().lock();
                selectContentSameAssert(sqlSelectPrimary, sqlSelectGSI, null, conn, conn);
            } finally {
                lock.writeLock().unlock();
            }

            System.out.println(Thread.currentThread().getName() + " run DML and check.");

            return inserted;
        }, throwException, 100));
    }

    private static final Consumer<Exception> throwException = (e) -> {
        throw GeneralUtil.nestedException(e);
    };

    private class InsertRunner implements Runnable {

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
    }

    void doClearDatabase() {
        JdbcUtil.executeUpdate(getTddlConnection1(), "use information_schema");
        String tddlSql = "drop database if exists " + DATABASE_NAME;
        JdbcUtil.executeUpdate(getTddlConnection1(), tddlSql);
    }
}
