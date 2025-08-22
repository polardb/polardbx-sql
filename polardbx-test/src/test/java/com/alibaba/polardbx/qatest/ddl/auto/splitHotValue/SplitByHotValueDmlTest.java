package com.alibaba.polardbx.qatest.ddl.auto.splitHotValue;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.qatest.ddl.auto.movepartition.MovePartitionDmlBaseTest;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

public class SplitByHotValueDmlTest extends MovePartitionDmlBaseTest {
    static private final String DATABASE_NAME = "SplitByHotValueDmlTest";

    private static final String PRIMARY_TABLE_NAME = "split_by_hot_value_primary";

    private static final String INDEX_NAME = "gsi_hot_value";

    private static final String SINGLE_PK_TMPL = "create table %s("
        + "id bigint not null auto_increment, "
        + "c1 bigint default null, "
        + "c2 varchar(256) default null, "
        + "v_a varchar(512) GENERATED ALWAYS AS (CONCAT(c1,' ',c2)) virtual,"
        + "v_b text GENERATED ALWAYS AS (CONCAT(c2,' ',c2)) stored,"
        + "v_c bigint GENERATED ALWAYS AS (c1 + 1) logical,"
        + "primary key(id),"
        + "key i_c2(c1, c2)"
        + ") %s";

    private static final String SINGLE_PK_TMPL_MYSQL = "create table %s("
        + "id bigint not null auto_increment, "
        + "c1 bigint default null, "
        + "c2 varchar(256) default null, "
        + "v_a varchar(512) GENERATED ALWAYS AS (CONCAT(c1,' ',c2)) virtual,"
        + "v_b text GENERATED ALWAYS AS (CONCAT(c2,' ',c2)) stored,"
        + "primary key(id),"
        + "key i_c2(c1, c2)"
        + ") %s";

    private static final String PARTITION_DEF1 = "partition by key(c2, id) partitions 2";

    private static final String CREATE_GSI_TMPL =
        "create global index {0} on {1}(c2, id) partition by key(c2, id) partitions 2";

    private static final String INSERT_TMPL = "insert into {0}(c1, c2) values(?, ?)";
    private static final String UPDATE_TMPL = "update {0} set c2=\"updated\" where id%2=0";
    private static final String DELETE_TMPL = "delete from {0} where id%3=0";

    private static final String ALTER_TABLE_GROUP_SPLIT_BY_HOT_VALUE_COMMAND =
        "alter tablegroup by table %s SPLIT INTO pp PARTITIONS 3 BY HOT VALUE('updated')";

    private static final String ALTER_TABLE_SPLIT_BY_HOT_VALUE_COMMAND =
        "alter table %s SPLIT INTO ppp PARTITIONS 4 BY HOT VALUE('updated')";

    private final String primaryShardingDef;

    protected static final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    @Parameterized.Parameters(name = "{index}:primaryShardingDef={0}")
    public static List<String[]> prepareDate() {
        return ImmutableList.of(new String[] {PARTITION_DEF1});
    }

    @Before
    public void before() {
        doReCreateDatabase();

        JdbcUtil.executeUpdateSuccess(mysqlConnection, "DROP TABLE IF EXISTS " + PRIMARY_TABLE_NAME);

        dropTableWithGsi(PRIMARY_TABLE_NAME, ImmutableList.of(INDEX_NAME));
    }

    public SplitByHotValueDmlTest(String primaryShardingDef) {
        super(DATABASE_NAME);
        this.primaryShardingDef = primaryShardingDef;
    }

    @Test
    public void singlePkMovePartitionDmlTest() throws SQLException {
        final String mysqlCreateTable = String.format(SINGLE_PK_TMPL_MYSQL, PRIMARY_TABLE_NAME, "");
        final String tddlCreateTable = String.format(SINGLE_PK_TMPL, PRIMARY_TABLE_NAME, primaryShardingDef);
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

        System.out.println("tablegroup split by hot value.");

        String tableGroupSplitHotValue =
            String.format(ALTER_TABLE_GROUP_SPLIT_BY_HOT_VALUE_COMMAND, PRIMARY_TABLE_NAME);
        System.out.println("command: " + tableGroupSplitHotValue);
        JdbcUtil.executeUpdateSuccess(tddlConnection, SLOW_HINT + tableGroupSplitHotValue);
        System.out.println("command end");

        System.out.println("tablegroup split by hot value done.");

        System.out.println("table split by hot value.");

        String tableSplitHotValue = String.format(ALTER_TABLE_SPLIT_BY_HOT_VALUE_COMMAND, PRIMARY_TABLE_NAME);
        System.out.println("command: " + tableSplitHotValue);
        JdbcUtil.executeUpdateSuccess(tddlConnection, SLOW_HINT + tableSplitHotValue);
        System.out.println("command end");

        System.out.println("table split by hot value done.");

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

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    public final ExecutorService dmlPool = Executors.newFixedThreadPool(10);

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
                if (notIgnoredErrors(e)) {
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
                if (notIgnoredErrors(e)) {
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
                if (notIgnoredErrors(e)) {
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
}
