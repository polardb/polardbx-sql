package com.alibaba.polardbx.qatest.ddl.auto.movepartition;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.qatest.CdcIgnore;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.esri.core.geometry.Point;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
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

public class MovePartitionPkTypeTest extends MovePartitionDmlBaseTest {
    static private final String DATABASE_NAME = "MovePartitionPkTypeTest";

    private static final String PRIMARY_TABLE_NAME = "move_part_pk_type_primary";

    private static final String PARTITION_GROUP = "p1";

    private static final String INSERT_TMPL = "insert into {0}(c1, c2, c3, c4) values(?, ?, ?, ?)";
    private static final String UPDATE_TMPL = "update {0} set c2 = null where c3%5=0";
    private static final String DELETE_TMPL = "delete from {0} where c3%7=0";

    private static final LocalDate START_DATE = LocalDate.of(1996, 1, 1);
    private static final LocalDate END_DATE = LocalDate.of(2026, 1, 1);

    private static final double MIN_LAT = -90.0;  // 最小纬度
    private static final double MAX_LAT = 90.0;   // 最大纬度
    private static final double MIN_LON = -180.0; // 最小经度
    private static final double MAX_LON = 180.0;  // 最大经度

    private static final String SINGLE_PK_TMPL = "create table %s("
        + "c1 %s, "
        + "c2 varchar(50) default 'abc', "
        + "c3 int,"
        + "c4 bigint,"
        + "v_a bigint GENERATED ALWAYS AS (c3 + c4) virtual,"
        + "v_b bigint GENERATED ALWAYS AS (c4 - c3) stored,"
        + "v_c varchar(100) GENERATED ALWAYS AS (CONCAT(c2,' ',c2)) logical,"
        + "primary key(c1)"
        + ") partition by key (c3) partitions 2";

    protected static final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public final ExecutorService dmlPool = Executors.newFixedThreadPool(10);

    public enum PkType {
        TINY,
        SHORT,
        INT,
        LONG,
        DECIMAL,
        FLOAT,
        DOUBLE,
        CHAR,
        VARCHAR,
        DATE,
        TIME,
        DATETIME,
        TIMESTAMP,
        TIME_H,
        DATETIME_H,
        TIMESTAMP_H,
        YEAR,
        BINARY,
        VARBINARY,
        GEOMETRY
    }

    private final PkType currentPkType;

    public MovePartitionPkTypeTest(PkType currentPkType) {
        super(DATABASE_NAME);
        this.currentPkType = currentPkType;
    }

    @Before
    public void before() {
        doReCreateDatabase();

        JdbcUtil.executeUpdateSuccess(mysqlConnection, "DROP TABLE IF EXISTS " + PRIMARY_TABLE_NAME);

        dropTableWithGsi(PRIMARY_TABLE_NAME, null);
    }

    @After
    public void after() {
        FailPoint.disable("FP_CATCHUP_TASK_SUSPEND");
    }

    @Parameterized.Parameters(name = "{index}:currentPkType={0}")
    public static List<PkType[]> prepareDate() {
        return ImmutableList.of(
            new PkType[] {PkType.TINY},
            new PkType[] {PkType.SHORT},
            new PkType[] {PkType.INT},
            new PkType[] {PkType.LONG},
            new PkType[] {PkType.DOUBLE},
            new PkType[] {PkType.FLOAT},
            new PkType[] {PkType.DECIMAL},
            new PkType[] {PkType.CHAR},
            new PkType[] {PkType.VARCHAR},
            new PkType[] {PkType.DATE},
            new PkType[] {PkType.TIME},
            new PkType[] {PkType.DATETIME},
            new PkType[] {PkType.TIMESTAMP},
            new PkType[] {PkType.TIME_H},
            new PkType[] {PkType.DATETIME_H},
            new PkType[] {PkType.TIMESTAMP_H},
            new PkType[] {PkType.YEAR},
            new PkType[] {PkType.BINARY},
            new PkType[] {PkType.VARBINARY}
            //new PkType[] {PkType.GEOMETRY} 该类型作为主键，backfill 等地方都不支持，没有做过兼容，不支持
        );
    }

    @Test
    @CdcIgnore(ignoreReason = "主键可能出现重复")
    public void movePartitionPkTypeTest() throws SQLException {
        final String tddlCreateTable = buildCreateTableSqlWithPkType(currentPkType);

        final String sqlInsert = MessageFormat.format(INSERT_TMPL, PRIMARY_TABLE_NAME);
        final String sqlUpdate = MessageFormat.format(UPDATE_TMPL, PRIMARY_TABLE_NAME);
        final String sqlDelete = MessageFormat.format(DELETE_TMPL, PRIMARY_TABLE_NAME);

        JdbcUtil.executeUpdateSuccess(tddlConnection, tddlCreateTable);

        final AtomicBoolean stop = new AtomicBoolean(false);
        final List<Future> inserts = new ArrayList<>();
        inserts.add(launchDmlCheckThread(sqlInsert, sqlUpdate, sqlDelete, stop, () -> 5, currentPkType));
        inserts.add(launchDmlCheckThread(sqlInsert, sqlUpdate, sqlDelete, stop, () -> 5, currentPkType));

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

    protected String buildCreateTableSqlWithPkType(PkType pkType) {
        String columnDef;
        switch (pkType) {
        case TINY:
            columnDef = "tinyint";
            break;
        case SHORT:
            columnDef = "smallint";
            break;
        case INT:
            columnDef = "int";
            break;
        case LONG:
            columnDef = "bigint";
            break;
        case DECIMAL:
            columnDef = "decimal(15,2)";
            break;
        case FLOAT:
            columnDef = "float(15,2)";
            break;
        case DOUBLE:
            columnDef = "double(15,2)";
            break;
        case CHAR:
            columnDef = "char(30)";
            break;
        case VARCHAR:
            columnDef = "varchar(30)";
            break;
        case DATE:
            columnDef = "date";
            break;
        case TIME:
            columnDef = "time";
            break;
        case DATETIME:
            columnDef = "datetime";
            break;
        case TIMESTAMP:
            columnDef = "timestamp";
            break;
        case TIME_H:
            columnDef = "time(3)";
            break;
        case DATETIME_H:
            columnDef = "datetime(3)";
            break;
        case TIMESTAMP_H:
            columnDef = "timestamp(3)";
            break;
        case YEAR:
            columnDef = "year";
            break;
        case BINARY:
            columnDef = "binary(20)";
            break;
        case VARBINARY:
            columnDef = "varbinary(20)";
            break;
        case GEOMETRY:
            columnDef = "point";
            break;
        default:
            throw new UnsupportedOperationException("Invalid type");
        }
        return String.format(SINGLE_PK_TMPL, PRIMARY_TABLE_NAME, columnDef);
    }

    protected ParameterContext buildRandomParameterContext(PkType pkType) {
        ParameterContext parameterContext;
        switch (pkType) {
        case TINY:
            parameterContext =
                new ParameterContext(ParameterMethod.setInt, new Object[] {1, RandomUtils.nextInt(0, 127)});
            break;
        case SHORT:
            parameterContext =
                new ParameterContext(ParameterMethod.setInt, new Object[] {1, RandomUtils.nextInt(0, 32767)});
            break;
        case INT:
            parameterContext =
                new ParameterContext(ParameterMethod.setInt, new Object[] {1, RandomUtils.nextInt()});
            break;
        case LONG:
            parameterContext = new ParameterContext(ParameterMethod.setLong, new Object[] {1, RandomUtils.nextLong()});
            break;
        case DECIMAL:
            parameterContext = new ParameterContext(ParameterMethod.setBigDecimal,
                new Object[] {1, BigDecimal.valueOf(RandomUtils.nextDouble(0, 1000000000))});
            break;
        case FLOAT:
            parameterContext = new ParameterContext(ParameterMethod.setFloat,
                new Object[] {1, RandomUtils.nextFloat(0, 1000000000)});
            break;
        case DOUBLE:
            parameterContext = new ParameterContext(ParameterMethod.setDouble,
                new Object[] {1, RandomUtils.nextDouble(0, 1000000000)});
            break;
        case CHAR:
        case VARCHAR:
            parameterContext = new ParameterContext(ParameterMethod.setString,
                new Object[] {1, RandomStringUtils.randomAlphabetic(30)});
            break;
        case DATE:
            long randomDays = RandomUtils.nextLong(0, ChronoUnit.DAYS.between(START_DATE, END_DATE) + 1);
            parameterContext = new ParameterContext(ParameterMethod.setObject1,
                new Object[] {1, START_DATE.plusDays(randomDays)});
            break;
        case TIME:
        case DATETIME:
        case TIMESTAMP:
        case TIME_H:
        case DATETIME_H:
        case TIMESTAMP_H:
            parameterContext = new ParameterContext(ParameterMethod.setObject1,
                new Object[] {1, new Date()});
            break;
        case YEAR:
            parameterContext =
                new ParameterContext(ParameterMethod.setInt, new Object[] {1, RandomUtils.nextInt(1970, 2026)});
            break;
        case BINARY:
        case VARBINARY:
            parameterContext = new ParameterContext(ParameterMethod.setBytes,
                new Object[] {1, RandomStringUtils.randomAlphabetic(20).getBytes()});
            break;
        case GEOMETRY:
            Random random = new Random();
            // 生成随机经度
            double lon = MIN_LON + (MAX_LON - MIN_LON) * random.nextDouble();
            // 生成随机纬度
            double lat = MIN_LAT + (MAX_LAT - MIN_LAT) * random.nextDouble();
            parameterContext = new ParameterContext(ParameterMethod.setObject1,
                new Object[] {1, new Point(lon, lat)});
            break;
        default:
            throw new UnsupportedOperationException("Invalid type");
        }

        return parameterContext;
    }

    protected Future<?> launchDmlCheckThread(String sqlInsert, String sqlUpdate, String sqlDelete,
                                             AtomicBoolean stop, Supplier<Integer> generateBatchSize,
                                             PkType pkType) {
        return dmlPool.submit(new InsertRunner(stop, (conn) -> {

            List<Map<Integer, ParameterContext>> batchParams = IntStream.range(0, generateBatchSize.get())
                .mapToObj(i -> ImmutableMap.<Integer, ParameterContext>builder()
                    .put(1, buildRandomParameterContext(pkType))
                    .put(2, new ParameterContext(ParameterMethod.setString,
                        new Object[] {2, RandomStringUtils.randomAlphabetic(30)}))
                    .put(3, new ParameterContext(ParameterMethod.setInt, new Object[] {3, RandomUtils.nextInt()}))
                    .put(4, new ParameterContext(ParameterMethod.setInt, new Object[] {4, RandomUtils.nextInt()}))
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

            System.out.println(Thread.currentThread().getName() + " run DML and check.");

            return inserted;
        }, throwException, 100));
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

    @Override
    protected void doReCreateDatabase() {
        doClearDatabase();
        String tddlSql = "use information_schema";
        JdbcUtil.executeUpdate(tddlConnection, tddlSql);
        tddlSql = "create database " + DATABASE_NAME + " partition_mode = 'auto'";
        JdbcUtil.executeUpdate(tddlConnection, tddlSql);
        tddlSql = "use " + DATABASE_NAME;
        JdbcUtil.executeUpdate(tddlConnection, tddlSql);
        JdbcUtil.executeUpdate(tddlConnection, "set sql_mode = ''");
    }

    @Override
    protected void doClearDatabase() {
        JdbcUtil.executeUpdate(getTddlConnection1(), "use information_schema");
        String tddlSql = "drop database if exists " + DATABASE_NAME;
        JdbcUtil.executeUpdate(getTddlConnection1(), tddlSql);
    }
}
