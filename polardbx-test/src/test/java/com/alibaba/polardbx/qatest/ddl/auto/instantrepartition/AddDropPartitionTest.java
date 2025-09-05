package com.alibaba.polardbx.qatest.ddl.auto.instantrepartition;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatterBuilder;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.qatest.CdcIgnore;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import org.junit.*;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class AddDropPartitionTest extends PartitionTestUtils {

    protected static final String currentDatabase = "add_drop_part_test";

    @Test
    @CdcIgnore(ignoreReason = "忽略主键冲突")
    public void testPartitionManagement() throws Exception {
        managePartitions();
    }

    @BeforeClass
    public static void beforeClass() {
        try (Connection connection = ConnectionManager.getInstance().newPolarDBXConnection()) {
            JdbcUtil.useDb(connection, "polardbx");
            JdbcUtil.dropDatabase(connection, currentDatabase);
            JdbcUtil.createPartDatabase(connection, currentDatabase);
            JdbcUtil.useDb(connection, currentDatabase);
            PartitionTestUtils.createTable(connection, true);
            PartitionTestUtils.createTable(connection, false);
            PartitionTestUtils.prepareData(connection, true, "2024-09-29 00:00:00", "2024-10-31 23:59:59", 20000);
            PartitionTestUtils.prepareData(connection, false, "2024-09-29 00:00:00", "2024-10-31 23:59:59", 10000);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Before
    public void before() throws Exception {
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }


    private void managePartitions() throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(3);
        Pair<List<List<String>>, List<List<String>>> dateInfo = preparePartitionNames("2024-10-31", "2024-09-30", 30);
        AtomicBoolean stop = new AtomicBoolean(false);
        Map<PartitionTestUtils.ErrorType, Pair<AtomicLong, String>> errorRecord = new HashMap<>();
        Runnable managePartitionsTask = () -> {
            try (Connection connection = ConnectionManager.getInstance().newPolarDBXConnection()) {
                int i = 0;
                JdbcUtil.useDb(connection, currentDatabase);
                while (!Thread.currentThread().isInterrupted()) {
                    removeOldestPartition(dateInfo.getValue().get(i), connection);
                    addNewestPartition(dateInfo.getKey().get(i), connection);
                    i++;
                    if (i >= dateInfo.getKey().size()) {
                        stop.set(true);
                        break;
                    }
                    Thread.sleep(TimeUnit.SECONDS.toMillis(5));
                }
            } catch (Exception e) {
                stop.set(true);
                e.printStackTrace();
                Assert.fail(e.getMessage());
            }
        };

        Runnable insertDataTask = () -> {
            try (Connection connection = ConnectionManager.getInstance().newPolarDBXConnection()) {
                JdbcUtil.useDb(connection, currentDatabase);
                Map<Long, Long> pkList = PartitionTestUtils.getPkList(connection, 10);
                int executionCount = 0;
                while (!stop.get()) {
                    for (Map.Entry<Long, Long> pkCount : pkList.entrySet()) {
                        executionCount++;
                        int phySql = 0;
                        List<List<String>> trace = null;
                        String insertSQL = PartitionTestUtils.genInsertIntoTarForSrcSQL(pkCount.getKey(), 10);
                        PreparedStatement ps = connection.prepareStatement("trace /*+TDDL:cmd_extra(MERGE_UNION=false)*/ " + insertSQL);
                        try {
                            ps.execute();
                        } catch (Exception ex) {
                            if (ex.getMessage().indexOf("ERR_PARTITION_NO_FOUND") != -1) {
                                if (errorRecord.containsKey(ErrorType.UNEXPECTED_ROUTE_ERROR)) {
                                    errorRecord.get(PartitionTestUtils.ErrorType.UNEXPECTED_ROUTE_ERROR).getKey().incrementAndGet();
                                } else {
                                    errorRecord.put(PartitionTestUtils.ErrorType.UNEXPECTED_ROUTE_ERROR, Pair.of(new AtomicLong(1), "ERR_PARTITION_NO_FOUND"));
                                }
                            } else if (ex.getMessage().indexOf("InterruptedException") != -1
                                    || ex.getMessage().indexOf("No operations allowed after connection closed") != -1
                                    || ex.getMessage().indexOf("Communications link failure") != -1) {
                                if (errorRecord.containsKey(ErrorType.UNEXPECTED_KILL)) {
                                    errorRecord.get(PartitionTestUtils.ErrorType.UNEXPECTED_KILL).getKey().incrementAndGet();
                                } else {
                                    errorRecord.put(PartitionTestUtils.ErrorType.UNEXPECTED_KILL, Pair.of(new AtomicLong(1), "kill"));
                                }
                            } else {
                                throw new RuntimeException(ex);
                            }
                        }
                        //JdbcUtil.executeUpdateSuccess(connection, "trace /*+TDDL:cmd_extra(MERGE_UNION=false)*/" + insertSQL);
                        trace = getTrace(connection);
                        phySql = 15 * 2 + pkCount.getValue().intValue();
                        if (trace.size() != phySql) {
                            if ((pkCount.getValue().intValue() > 1 && trace.size() > phySql) || (pkCount.getValue().intValue() == 1 && trace.size() != phySql)) {
                                if (errorRecord.containsKey(PartitionTestUtils.ErrorType.UNEXPECTED_Double_Write)) {
                                    errorRecord.get(PartitionTestUtils.ErrorType.UNEXPECTED_Double_Write).getKey().incrementAndGet();
                                } else {
                                    errorRecord.put(PartitionTestUtils.ErrorType.UNEXPECTED_Double_Write, Pair.of(new AtomicLong(1), "expect physical sql " + phySql + " but trace:" + trace.toString()));
                                }
                            }
                            //Assert.assertThat(trace.toString(), trace.size(), pkCount.getValue().intValue() > 1 ? lessThanOrEqualTo(phySql) : is(phySql));
                        }
                        if (stop.get()) {
                            break;
                        }

                        //Assert.assertThat(trace.toString(), trace.size(), pkCount.getValue().intValue() > 1 ? lessThanOrEqualTo(phySql) : is(phySql));
                        if ((executionCount % 20 == 0) && RandomUtils.getBoolean()) {
                            ps = connection.prepareStatement(PartitionTestUtils.genDeleteFromTarSQL(pkCount.getKey()));
                            try {
                                ps.execute();
                            } catch (Exception ex) {
                                if (ex.getMessage().indexOf("ERR_PARTITION_NO_FOUND") != -1) {
                                    if (errorRecord.containsKey(ErrorType.UNEXPECTED_ROUTE_ERROR)) {
                                        errorRecord.get(PartitionTestUtils.ErrorType.UNEXPECTED_ROUTE_ERROR).getKey().incrementAndGet();
                                    } else {
                                        errorRecord.put(PartitionTestUtils.ErrorType.UNEXPECTED_ROUTE_ERROR, Pair.of(new AtomicLong(1), "expect physical sql " + phySql + " but trace:" + trace == null ? "" : trace.toString()));
                                    }
                                } else if (ex.getMessage().indexOf("InterruptedException") != -1
                                        || ex.getMessage().indexOf("No operations allowed after connection closed") != -1
                                        || ex.getMessage().indexOf("Communications link failure") != -1) {
                                    if (errorRecord.containsKey(ErrorType.UNEXPECTED_KILL)) {
                                        errorRecord.get(PartitionTestUtils.ErrorType.UNEXPECTED_KILL).getKey().incrementAndGet();
                                    } else {
                                        errorRecord.put(PartitionTestUtils.ErrorType.UNEXPECTED_KILL, Pair.of(new AtomicLong(1), "kill"));
                                    }
                                } else {
                                    throw new RuntimeException(ex);
                                }
                            }
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                Assert.fail(e.getMessage());
            }
        };


        Runnable selectForUpdateTask = () -> {

            try (Connection connection = ConnectionManager.getInstance().newPolarDBXConnection()) {
                JdbcUtil.useDb(connection, currentDatabase);
                Map<Long, Long> pkList = PartitionTestUtils.getPkList(connection, 10);
                int executionCount = 0;
                while (!stop.get()) {
                    for (Map.Entry<Long, Long> pkCount : pkList.entrySet()) {
                        try {
                            executionCount++;
                            connection.setAutoCommit(false);
                            PreparedStatement ps = connection.prepareStatement(PartitionTestUtils.genSelectForUpdateFromTarSQL(pkCount.getKey()));
                            ps.execute();
                            if (executionCount % 100 == 0) {
                                Thread.sleep(TimeUnit.SECONDS.toMillis(RandomUtils.getIntegerBetween(10, 20)));
                            }
                            connection.commit();
                            if (stop.get()) {
                                break;
                            }
                        } catch (Exception ex) {
                            if (ex.getMessage().indexOf("InterruptedException") != -1
                                    || ex.getMessage().indexOf("No operations allowed after connection closed") != -1
                                    || ex.getMessage().indexOf("Communications link failure") != -1) {
                                if (errorRecord.containsKey(ErrorType.UNEXPECTED_KILL)) {
                                    errorRecord.get(PartitionTestUtils.ErrorType.UNEXPECTED_KILL).getKey().incrementAndGet();
                                } else {
                                    errorRecord.put(PartitionTestUtils.ErrorType.UNEXPECTED_KILL, Pair.of(new AtomicLong(1), "kill"));
                                }
                            }
                        } finally {
                            connection.setAutoCommit(true);
                        }
                    }
                }
            } catch (Throwable t) {
                throw new RuntimeException(t);
            }
        };

        try {
            final List<Future> dmlTasks = new ArrayList<>();
            dmlTasks.add(executor.submit(insertDataTask));
            dmlTasks.add(executor.submit(managePartitionsTask));
            dmlTasks.add(executor.submit(selectForUpdateTask));
            for (Future future : dmlTasks) {
                try {
                    future.get();
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        } finally {
            executor.shutdownNow();
        }
        for (Map.Entry<ErrorType, Pair<AtomicLong, String>> entry : errorRecord.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue().getValue());
        }
        if (errorRecord.size() > 0) {
            Assert.assertThat(errorRecord.toString(), errorRecord.size(), is(0));
        }
    }

    private void removeOldestPartition(List<String> oldPartitions, Connection conn) throws Exception {
        Statement stmt = conn.createStatement();
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE c DROP SUBPARTITION ");
        for (int i = 0; i < oldPartitions.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(oldPartitions.get(i));
        }
        stmt.executeUpdate(sb.toString());
        stmt.close();
    }

    private void addNewestPartition(List<String> newDays, Connection conn) throws Exception {
        Statement stmt = conn.createStatement();
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE c ADD SUBPARTITION (");
        for (int i = 0; i < newDays.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append("SUBPARTITION ");
            sb.append(getPartNameFromDate(parseDateFromPartName(newDays.get(i))));
            sb.append(" VALUES LESS THAN (TO_DAYS('");
            sb.append(dateToString(parseDateFromPartName(newDays.get(i)).plusDays(1)));
            sb.append("'))");
        }
        sb.append(")");
        stmt.executeUpdate(sb.toString());
    }

    private String getPartitionName(Date date, int offset) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.DAY_OF_MONTH, offset);
        return "p" + new DateTimeFormatterBuilder()
                .appendPattern("yyyyMMdd")
                .toFormatter()
                .format(LocalDateTime.ofInstant(calendar.toInstant(), java.time.ZoneId.systemDefault()));
    }

    private Date getTomorrow(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.DAY_OF_MONTH, 1);
        return calendar.getTime();
    }

    private Date getCurrentDate() {
        return new Date();
    }
}
