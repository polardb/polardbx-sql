package com.alibaba.polardbx.qatest.dql.auto.function;

import com.alibaba.polardbx.common.lock.LockingConfig;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.server.lock.LockingFunctionManager;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.validator.DataValidator;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@RunWith(Parameterized.class)
public class LockingFunctionTest extends ReadBaseTestCase {
    private static ExecutorService threadPool;
    private Connection[] tddlConnections = new Connection[3];
    private Connection mysqlConnection1 = null;

    @Parameterized.Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepare() {
        return Arrays.asList(ExecuteTableSelect.selectBaseOneBaseTwoMutilDbMutilTb());
    }

    public LockingFunctionTest(String baseOneTableName, String baseTwoTableName) {
        this.baseOneTableName = baseOneTableName;
        this.baseTwoTableName = baseTwoTableName;
    }

    @BeforeClass
    public static void preEnv() throws SQLException {
        threadPool = new ThreadPoolExecutor(8, 8, 0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>(),
            new NamedThreadFactory("locking-function-test", false));
        JdbcUtil.executeSuccess(ConnectionManager.getInstance().getDruidMetaConnection(),
            "truncate table " + GmsSystemTables.LOCKING_FUNCTIONS);
    }

    @AfterClass
    public static void shutdownThreadPool() {
        if (threadPool != null) {
            threadPool.shutdownNow();
        }
    }

    /**
     * create a new connection, rather than getting connections from pool.
     */
    @Before
    public void createConnection() {
        for (int i = 0; i < 3; i++) {
            tddlConnections[i] = getPolardbxConnection();
        }
        mysqlConnection1 = getMysqlConnection();
    }

    @After
    public void closeConnection() throws Exception {
        for (int i = 0; i < 3; i++) {
            Connection connection = tddlConnections[i];
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        }

        mysqlConnection1.close();
    }

    @Test
    public void testLockGetAndRelease() throws SQLException {
        try (Connection tddlConnection1 = tddlConnections[0]) {
            DataValidator
                .selectContentSameAssert("SELECT GET_LOCK('lock1',10)", null, mysqlConnection1, tddlConnection1);
            DataValidator
                .selectContentSameAssert("SELECT GET_LOCK('lock2',10)", null, mysqlConnection1, tddlConnection1);
            DataValidator
                .selectContentSameAssert("SELECT RELEASE_LOCK('lock2')", null, mysqlConnection1, tddlConnection1);
            DataValidator
                .selectContentSameAssert("SELECT RELEASE_LOCK('lock1')", null, mysqlConnection1, tddlConnection1);
            DataValidator
                .selectContentSameAssert("SELECT RELEASE_LOCK('lock1')", null, mysqlConnection1, tddlConnection1);
            DataValidator
                .selectContentSameAssert("SELECT RELEASE_LOCK('lock2')", null, mysqlConnection1, tddlConnection1);
        }
    }

    @Test
    public void testReleaseAllLocks() throws Exception {
        try (Connection tddlConnection1 = tddlConnections[0]) {
            DataValidator
                .selectContentSameAssert("SELECT RELEASE_ALL_LOCKS()", null, mysqlConnection1, tddlConnection1);
            DataValidator
                .selectContentSameAssert("SELECT GET_LOCK('lock1',10)", null, mysqlConnection1, tddlConnection1);
            DataValidator
                .selectContentSameAssert("SELECT GET_LOCK('lock2',10)", null, mysqlConnection1, tddlConnection1);
            DataValidator
                .selectContentSameAssert("SELECT GET_LOCK('lock2',10)", null, mysqlConnection1, tddlConnection1);
            DataValidator
                .selectContentSameAssert("SELECT GET_LOCK('lock3',10)", null, mysqlConnection1, tddlConnection1);
            DataValidator
                .selectContentSameAssert("SELECT GET_LOCK('lock3',10)", null, mysqlConnection1, tddlConnection1);
            DataValidator
                .selectContentSameAssert("SELECT GET_LOCK('lock3',10)", null, mysqlConnection1, tddlConnection1);
            DataValidator
                .selectContentSameAssert("SELECT GET_LOCK('lock3',10)", null, mysqlConnection1, tddlConnection1);
            DataValidator
                .selectContentSameAssert("SELECT RELEASE_ALL_LOCKS()", null, mysqlConnection1, tddlConnection1);
            DataValidator
                .selectContentSameAssert("SELECT RELEASE_ALL_LOCKS()", null, mysqlConnection1, tddlConnection1);
        }
    }

    @Test
    public void testReentrantLock() throws Exception {
        try (Connection tddlConnection1 = tddlConnections[0]) {
            DataValidator
                .selectContentSameAssert("SELECT GET_LOCK('lock1',10)", null, mysqlConnection1, tddlConnection1);
            DataValidator
                .selectContentSameAssert("SELECT GET_LOCK('lock1',10)", null, mysqlConnection1, tddlConnection1);
            DataValidator
                .selectContentSameAssert("SELECT RELEASE_LOCK('lock1')", null, mysqlConnection1, tddlConnection1);
            DataValidator
                .selectContentSameAssert("SELECT RELEASE_LOCK('lock1')", null, mysqlConnection1, tddlConnection1);
            DataValidator
                .selectContentSameAssert("SELECT RELEASE_LOCK('lock1')", null, mysqlConnection1, tddlConnection1);
        }
    }

    @Test
    public void testSeizeLock() throws Exception {
        Semaphore semaphore = new Semaphore(0);
        Future f1 = threadPool.submit(() -> {
            try (Connection tddlConnection1 = tddlConnections[0]) {
                executeSuccess(tddlConnection1, "SELECT GET_LOCK('lock1',10)", "1");
                semaphore.release(1);
                executeSuccess(tddlConnection1, "SELECT RELEASE_LOCK('lock1')", "1");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
        Future f2 = threadPool.submit(() -> {
            try (Connection tddlConnection2 = tddlConnections[1]) {
                semaphore.acquire(1);
                executeSuccess(tddlConnection2, "SELECT GET_LOCK('lock1'， 10)", "1");
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        });
        f1.get(200, TimeUnit.SECONDS);
        f2.get(200, TimeUnit.SECONDS);
    }

    private void getAndReleaseLock(String lockName, Connection connection) {
        //加锁有可能因为metadb死锁导致返回null
        String res1 =
            JdbcUtil.executeQueryAndGetFirstStringResult("SELECT GET_LOCK('" + lockName + "',-1)", connection);
        String res2 =
            JdbcUtil.executeQueryAndGetFirstStringResult("SELECT RELEASE_LOCK('" + lockName + "')", connection);
        if ("1".equals(res1)) {
            Assert.assertTrue("1".equals(res2));
        } else {
            Assert.assertTrue(!"1".equals(res2));
        }
    }

    @Test
    public void testSeizeLock2() throws Exception {
        int lockNum = 10;
        int sessionCount = 50;

        ExecutorService pool = Executors.newFixedThreadPool(sessionCount);
        Connection conn = getPolardbxConnection();

        List<Future> futures = new ArrayList<>();
        for (int i = 0; i < sessionCount; i++) {
            final int threadIndex = i;
            futures.add(pool.submit(() -> {
                try (Connection connection = getPolardbxConnection()) {
                    int lockIndex = threadIndex % lockNum;
                    getAndReleaseLock("lock" + lockIndex, connection);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }));
        }

        for (Future f : futures) {
            f.get(100, TimeUnit.SECONDS);
        }
        conn.close();
        pool.shutdownNow();
    }

    @Test
    public void testSeizeLock3() throws Exception {
        int sessionCount = 50;
        ExecutorService pool = Executors.newFixedThreadPool(sessionCount);
        Connection conn = getPolardbxConnection();

        List<Future> futures = new ArrayList<>();
        for (int i = 0; i < sessionCount; i++) {
            futures.add(pool.submit(() -> {
                try (Connection connection = getPolardbxConnection()) {
                    getAndReleaseLock("lock1", connection);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }));
        }

        for (Future f : futures) {
            f.get(100, TimeUnit.SECONDS);
        }
        pool.shutdownNow();
    }

    @Test
    public void testSeizeLockAndDeadLock() throws Exception {
        int sessionCount = 20;
        ExecutorService pool = Executors.newFixedThreadPool(sessionCount);
        AtomicInteger lockSuccessCount = new AtomicInteger(0);
        AtomicInteger deadLockCount = new AtomicInteger(0);
        List<Future> futures = new ArrayList<>();
        for (int i = 0; i < sessionCount; i++) {
            final int threadIndex = i;
            futures.add(pool.submit(() -> {
                try (Connection connection = getPolardbxConnection()) {
                    List<String> lockNames = new ArrayList<>();
                    for (int j = 0; j < 5; j++) {
                        String lockName = "lock" + ThreadLocalRandom.current().nextInt(10);
                        ResultSet rs = null;
                        try {
                            rs = executeQueryExceptionThrowable("select get_lock('" + lockName + "', -1)", connection);
                            Assert.assertTrue(rs.next());
                            String res = rs.getString(1);
                            if (res != null) {
                                Assert.assertTrue(res.equals("1"));
                                lockNames.add(lockName);
                                lockSuccessCount.incrementAndGet();
                            }
                            rs.close();
                        } catch (Exception e) {
                            Assert.assertTrue(e.getMessage().contains("ERR_USER_LOCK_DEADLOCK"));
                            deadLockCount.incrementAndGet();
                        } finally {
                            if (rs != null) {
                                rs.close();
                            }
                        }
                    }
                    for (String lockName : lockNames) {
                        executeSuccess(connection, "select release_lock('" + lockName + "')", "1");
                    }

                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }));
        }
        for (Future f : futures) {
            f.get(100, TimeUnit.SECONDS);
        }
        pool.shutdownNow();
        System.out.println("lock success count : " + lockSuccessCount);
        System.out.println("deadLock count : " + deadLockCount);

    }

    /**
     * when connection closed, all locks hold by this connection will be released.
     */
    @Test
    public void testConnectionClose() throws Exception {
        Semaphore semaphore = new Semaphore(0);
        Future[] futures = new Future[2];
        futures[0] = threadPool.submit(() -> {
            Object rs = null;
            try (Connection tddlConnection1 = tddlConnections[0]) {
                executeSuccess(tddlConnection1, "SELECT GET_LOCK('lock1',10)", "1");
                executeSuccess(tddlConnection1, "SELECT GET_LOCK('lock2',10)", "1");
                semaphore.release(1);
            }
            return rs;
        });
        futures[1] = threadPool.submit(() -> {
            Object rs = null;
            try (Connection tddlConnection2 = tddlConnections[1]) {
                semaphore.acquire(1);
                executeSuccess(tddlConnection2, "SELECT GET_LOCK('lock1',100)", "1");
                executeSuccess(tddlConnection2, "SELECT GET_LOCK('lock2',100)", "1");

            } catch (Exception e) {
                rs = e;
            }
            return rs;
        });
        futures[0].get(100, TimeUnit.SECONDS);
        futures[1].get(100, TimeUnit.SECONDS);
    }

    /**
     * typical deadlock scene:
     * session1: get_lock(lock1), get_lock(lock2)
     * session2: get_lock(lock2), get_lock(lock1)
     */
    @Test
    public void testDeadlock1() throws Exception {
        Semaphore semaphore1 = new Semaphore(0);
        Semaphore semaphore2 = new Semaphore(0);
        Future[] futures = new Future[2];
        futures[0] = threadPool.submit(() -> {
            Object rs = null;
            try (Connection tddlConnection1 = tddlConnections[0]) {
                JdbcUtil.executeSuccess(tddlConnection1, "select get_lock('lock1', 10)");
                semaphore1.release(1);
                semaphore2.acquire(1);
                rs = executeQueryExceptionThrowable("select get_lock('lock2', 10)", tddlConnection1);
            } catch (Exception e) {
                rs = e;
            }
            return rs;
        });
        futures[1] = threadPool.submit(() -> {
            Object rs = null;
            try (Connection tddlConnection2 = tddlConnections[1]) {
                JdbcUtil.executeSuccess(tddlConnection2, "select get_lock('lock2', 10)");
                semaphore2.release(1);
                semaphore1.acquire(1);
                rs = executeQueryExceptionThrowable("select get_lock('lock1', 10)", tddlConnection2);
            } catch (SQLException e) {
                rs = e;
            }
            return rs;
        });

        boolean deadlock = false;
        for (Future f : futures) {
            // must to wait for all tasks being finished.
            Object o = f.get(60, TimeUnit.SECONDS);
            if (o instanceof Exception &&
                ((Exception) o).getMessage().contains("ERR_USER_LOCK_DEADLOCK")) {
                deadlock = true;
            }
        }
        Assert.assertTrue(deadlock);
    }

    /**
     * typical deadlock scene:
     * session1: get_lock(lock1), get_lock(lock2)
     * session2: get_lock(lock2), get_lock(lock3)
     * session3: get_lock(lock3), get_lock(lock1)
     */
    @Test
    public void testDeadlock2() throws Exception {
        Semaphore semaphore1 = new Semaphore(0);
        Semaphore semaphore2 = new Semaphore(0);
        Semaphore semaphore3 = new Semaphore(0);
        Future[] futures = new Future[3];
        futures[0] = threadPool.submit(() -> {
            Object rs = null;
            try (Connection tddlConnection1 = tddlConnections[0]) {
                JdbcUtil.executeQuery("select get_lock('lock1', 10)", tddlConnection1);
                semaphore1.release(1);
                semaphore2.acquire(1);
                rs = executeQueryExceptionThrowable("select get_lock('lock2', 10)", tddlConnection1);
            } catch (Exception e) {
                rs = e;
            }
            return rs;
        });
        futures[1] = threadPool.submit(() -> {
            Object rs = null;
            try (Connection tddlConnection2 = tddlConnections[1]) {
                JdbcUtil.executeQuery("select get_lock('lock2', 10)", tddlConnection2);
                semaphore2.release(1);
                semaphore3.acquire(1);
                rs = executeQueryExceptionThrowable("select get_lock('lock3', 10)", tddlConnection2);
            } catch (SQLException e) {
                rs = e;
            }
            return rs;
        });
        futures[2] = threadPool.submit(() -> {
            Object rs = null;
            try (Connection tddlConnection3 = tddlConnections[2]) {
                JdbcUtil.executeQuery("select get_lock('lock3', 10)", tddlConnection3);
                semaphore3.release(1);
                semaphore1.acquire(1);
                rs = executeQueryExceptionThrowable("select get_lock('lock1', 10)", tddlConnection3);
            } catch (SQLException e) {
                rs = e;
            }
            return rs;
        });
        boolean deadlock = false;
        for (Future f : futures) {
            // must to wait for all tasks being finished.
            Object o = f.get(60, TimeUnit.SECONDS);
            if (o instanceof Exception &&
                ((Exception) o).getMessage().contains("ERR_USER_LOCK_DEADLOCK")) {
                deadlock = true;
            }
        }
        Assert.assertTrue(deadlock);
    }

    /**
     * typical no deadlock scene:
     * session1: get_lock(lock1), get_lock(lock2)
     * session2: get_lock(lock2), get_lock(lock3)
     */
    @Test
    public void testNoDeadlock1() throws Exception {
        Semaphore semaphore1 = new Semaphore(0);
        Semaphore semaphore2 = new Semaphore(0);
        Future[] futures = new Future[2];
        futures[0] = threadPool.submit(() -> {
            Object rs = null;
            try (Connection tddlConnection1 = tddlConnections[0]) {
                JdbcUtil.executeQuery("select get_lock('lock1', 10)", tddlConnection1);
                semaphore1.release(1);
                semaphore2.acquire(1);
                rs = executeQueryExceptionThrowable("select get_lock('lock2', 10)", tddlConnection1);
            } catch (Exception e) {
                rs = e;
            }
            return rs;
        });
        futures[1] = threadPool.submit(() -> {
            Object rs = null;
            try (Connection tddlConnection2 = tddlConnections[1]) {
                JdbcUtil.executeQuery("select get_lock('lock2', 10)", tddlConnection2);
                semaphore2.release(1);
                semaphore1.acquire(1);
                rs = executeQueryExceptionThrowable("select get_lock('lock3', 10)", tddlConnection2);
            } catch (SQLException e) {
                rs = e;
            }
            return rs;
        });

        boolean deadlock = false;
        for (Future f : futures) {
            // must to wait for all tasks being finished.
            Object o = f.get(60, TimeUnit.SECONDS);
            if (o instanceof Exception &&
                ((Exception) o).getMessage().contains("ERR_USER_LOCK_DEADLOCK")) {
                deadlock = true;
            }
        }
        Assert.assertTrue(!deadlock);
    }

    /**
     * typical no deadlock scene:
     * session1: get_lock(lock1), get_lock(lock2)
     * session2: get_lock(lock2), get_lock(lock3)
     * session3: get_lock(lock3), get_lock(lock4)
     */
    @Test
    public void testNoDeadlock2() throws Exception {
        Semaphore semaphore1 = new Semaphore(0);
        Semaphore semaphore2 = new Semaphore(0);
        Semaphore semaphore3 = new Semaphore(0);
        Future[] futures = new Future[3];
        futures[0] = threadPool.submit(() -> {
            Object rs = null;
            try (Connection tddlConnection1 = tddlConnections[0]) {
                JdbcUtil.executeQuery("select get_lock('lock1', 10)", tddlConnection1);
                semaphore1.release(1);
                semaphore2.acquire(1);
                rs = executeQueryExceptionThrowable("select get_lock('lock2', 10)", tddlConnection1);
            } catch (Exception e) {
                rs = e;
            }
            return rs;
        });
        futures[1] = threadPool.submit(() -> {
            Object rs = null;
            try (Connection tddlConnection2 = tddlConnections[1]) {
                JdbcUtil.executeQuery("select get_lock('lock2', 10)", tddlConnection2);
                semaphore2.release(1);
                semaphore3.acquire(1);
                rs = executeQueryExceptionThrowable("select get_lock('lock3', 10)", tddlConnection2);
            } catch (SQLException e) {
                rs = e;
            }
            return rs;
        });
        futures[2] = threadPool.submit(() -> {
            Object rs = null;
            try (Connection tddlConnection3 = tddlConnections[2]) {
                JdbcUtil.executeQuery("select get_lock('lock3', 10)", tddlConnection3);
                semaphore3.release(1);
                semaphore1.acquire(1);
                rs = executeQueryExceptionThrowable("select get_lock('lock4', 10)", tddlConnection3);
            } catch (SQLException e) {
                rs = e;
            }
            return rs;
        });
        boolean deadlock = false;
        for (Future f : futures) {
            // must to wait for all tasks being finished.
            Object o = f.get(60, TimeUnit.SECONDS);
            if (o instanceof Exception &&
                ((Exception) o).getMessage().contains("ERR_USER_LOCK_DEADLOCK")) {
                deadlock = true;
            }
        }
        Assert.assertTrue(!deadlock);
    }

    @Test
    public void testIsFreeLock() throws SQLException {
        try (Connection tddlConnection1 = tddlConnections[0]) {
            DataValidator
                .selectContentSameAssert("SELECT GET_LOCK('lock1',10)", null, mysqlConnection1, tddlConnection1);
            DataValidator
                .selectContentSameAssert("SELECT IS_FREE_LOCK('lock1')", null, mysqlConnection1, tddlConnection1);
            DataValidator
                .selectContentSameAssert("SELECT RELEASE_LOCK('lock1')", null, mysqlConnection1, tddlConnection1);
            DataValidator
                .selectContentSameAssert("SELECT IS_FREE_LOCK('lock1')", null, mysqlConnection1, tddlConnection1);
        }
    }

    @Test
    public void testIsUsedLock() throws SQLException {
        try (Connection tddlConnection1 = tddlConnections[0]) {
            JdbcUtil.executeSuccess(tddlConnection1, "SELECT GET_LOCK('lock1',10)");
            String sessionInfo = null;
            ResultSet rs = JdbcUtil.executeQuery("SELECT IS_USED_LOCK('lock1') AS SESSION_INFO", tddlConnection1);
            while (rs.next()) {
                sessionInfo = rs.getString("SESSION_INFO");
            }
            Assert.assertTrue(sessionInfo != null);
            JdbcUtil.executeSuccess(tddlConnection1, "SELECT RELEASE_LOCK('lock1')");
            rs = JdbcUtil.executeQuery("SELECT IS_USED_LOCK('lock1') AS SESSION_INFO", tddlConnection1);
            while (rs.next()) {
                sessionInfo = rs.getString("SESSION_INFO");
            }
            Assert.assertTrue(sessionInfo == null);
        }
    }

    @Test
    public void testLease() throws Exception {

        Semaphore semaphore1 = new Semaphore(0);
        Semaphore semaphore2 = new Semaphore(0);

        Future[] futures = new Future[2];
        futures[0] = threadPool.submit(() -> {
            Object rs = null;
            try (Connection tddlConnection1 = tddlConnections[0]) {
                executeSuccess(tddlConnection1, "SELECT GET_LOCK('lock1',10)", "1");
                semaphore1.release(1);
                semaphore2.acquire(1);

            }
            return rs;
        });
        futures[1] = threadPool.submit(() -> {
            Object rs = null;
            try (Connection tddlConnection2 = tddlConnections[1]) {
                semaphore1.acquire(1);
                try (Connection metaDbConnection = getMetaConnection()) {
                    //强制将lock1的sessionId修改，则tddlConnection1的心跳会被终止
                    //同时修改gmt_modified，导致其已经过期
                    PreparedStatement ps = metaDbConnection.prepareStatement(
                        "update " + GmsSystemTables.LOCKING_FUNCTIONS + " set session_id = 'xxxx', "
                            + "gmt_modified = date_sub(gmt_modified, interval " + (
                            LockingConfig.EXPIRATION_TIME + 1) + " second) "
                            + "where lock_name = 'lock1'");
                    ps.executeUpdate();
                }
                executeSuccess(tddlConnection2, "SELECT GET_LOCK('lock1',-1)", "1");
                semaphore2.release(1);
            } catch (Exception e) {
                rs = e;
            }
            return rs;
        });
        futures[0].get(100, TimeUnit.SECONDS);
        futures[1].get(100, TimeUnit.SECONDS);

    }

    @Test
    public void testAcquireLockTimeout() throws Exception {
        Semaphore semaphore1 = new Semaphore(0);
        Semaphore semaphore2 = new Semaphore(0);
        Future[] futures = new Future[2];
        futures[0] = threadPool.submit(() -> {
            Object rs = null;
            try (Connection tddlConnection1 = tddlConnections[0]) {
                executeSuccess(tddlConnection1, "select get_lock('lock1', 10)", "1");
                semaphore1.release(1);
                semaphore2.acquire(1);
            } catch (Exception e) {
                rs = e;
            }
            return rs;
        });
        futures[1] = threadPool.submit(() -> {
            Object rs = null;
            try (Connection tddlConnection2 = tddlConnections[1]) {
                semaphore1.acquire(1);
                executeSuccess(tddlConnection2, "select get_lock('lock1', 5)", "0");
                semaphore2.release(1);
            } catch (SQLException e) {
                rs = e;
            }
            return rs;
        });
        for (Future f : futures) {
            f.get(100, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testLockHeartBeat() throws Exception {
        int lockNum = 10;
        try (Connection tddlConnection1 = tddlConnections[0]) {
            for (int i = 0; i < lockNum; i++) {
                String lockName = "lock" + i;
                executeSuccess(tddlConnection1, "select get_lock('" + lockName + "', 10)", "1");
            }
            Thread.sleep(LockingConfig.HEART_BEAT_INTERVAL + 2000);
            try (Connection metaDbConnection = getMetaConnection()) {
                for (int i = 0; i < lockNum; i++) {
                    String lockName = "lock" + i;
                    String sql =
                        "select gmt_created < gmt_modified from " +
                            GmsSystemTables.LOCKING_FUNCTIONS + " where lock_name='" + lockName + "'";
                    String res = JdbcUtil.executeQueryAndGetFirstStringResult(sql, metaDbConnection);
                    Assert.assertTrue("1".equalsIgnoreCase(res));
                }
            }
        }
    }

    @Test
    public void testIsMyLock() throws Exception {
        Future[] futures = new Future[2];
        futures[0] = threadPool.submit(() -> {
            Object rs = null;
            try (Connection tddlConnection1 = tddlConnections[0]) {
                executeSuccess(tddlConnection1, "select get_lock('lock1', 10)", "1");
                executeSuccess(tddlConnection1, "select is_my_lock('lock1')", "1");
            } catch (Exception e) {
                rs = e;
            }
            return rs;
        });
        futures[1] = threadPool.submit(() -> {
            Object rs = null;
            try (Connection tddlConnection2 = tddlConnections[1]) {
                executeSuccess(tddlConnection2, "select get_lock('lock1', 10)", "1");
                executeSuccess(tddlConnection2, "select is_my_lock('lock1')", "1");
            } catch (SQLException e) {
                rs = e;
            }
            return rs;
        });
        executeSuccess(tddlConnections[2], "select is_my_lock('lock1')", "0");

        for (Future f : futures) {
            f.get(100, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testGetAllLocks() throws Exception {
        StringJoiner sj = new StringJoiner(",");
        try (Connection tddlConnection1 = tddlConnections[0]) {
            for (int i = 0; i < 10; i++) {
                String lockName = "lock" + i;
                sj.add(lockName);
                executeSuccess(tddlConnection1, "select get_lock('" + lockName + "', 10)", "1");
            }
            executeSuccess(tddlConnection1, "select all_my_lock()", sj.toString());
        }
    }

    private ResultSet executeQueryExceptionThrowable(String sql, Connection c) throws SQLException {
        Statement ps = JdbcUtil.createStatement(c);
        ResultSet rs = ps.executeQuery(sql);
        return rs;
    }

    private void executeSuccess(Connection connection, String sql, String expect) {
        String res = JdbcUtil.executeQueryAndGetFirstStringResult(sql, connection);
        Assert.assertTrue(expect.equals(res));
    }

}