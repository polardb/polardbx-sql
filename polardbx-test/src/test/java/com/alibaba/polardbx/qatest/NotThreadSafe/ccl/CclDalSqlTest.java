package com.alibaba.polardbx.qatest.NotThreadSafe.ccl;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;
import lombok.SneakyThrows;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author busu
 * date: 2020/11/6 5:17 下午
 */
public class CclDalSqlTest extends ReadBaseTestCase {

    static String keyword = "dingfengdingfengdingfengdingfengdingfeng";

    static String testNoMatchCacheDbName = "ccl_test_sdfsalda";

    static String userName = "polardbx_root";

    final static String CCL_TEST_TABLE_NAME = "ccl_test_tb";

    @BeforeClass
    public static void beforeClass() throws Exception {
        Connection connection = ConnectionManager.getInstance().newPolarDBXConnection();
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("clear ccl_triggers");
            stmt.execute("create database if not exists " + testNoMatchCacheDbName);
            stmt.execute("use " + testNoMatchCacheDbName);
            stmt.execute("create table if not exists " + CCL_TEST_TABLE_NAME
                + "(id int not null, myname char(50) not null, primary key(id)) dbpartition by hash(id)");
            stmt.execute("clear ccl_rules");
        }
        connection.close();
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        Connection connection = ConnectionManager.getInstance().newPolarDBXConnection();
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("clear ccl_rules");
            stmt.execute("use " + PropertiesUtil.polardbXShardingDBName1());
            stmt.execute("clear ccl_triggers");
            stmt.execute("drop database if exists " + testNoMatchCacheDbName);
        }
        connection.close();
    }

    @Before
    public void before() throws SQLException {
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("clear ccl_triggers");
            stmt.execute("use " + testNoMatchCacheDbName);
            stmt.execute("clear ccl_rules");
            stmt.execute("CREATE CCL_RULE  if not exists busu1118 ON `*`.`*` TO '" + userName + "'@'%' "
                + "             FOR SELECT " + "             FILTER BY KEYWORD('" + keyword + "') "
                + "             WITH MAX_CONCURRENCY=0");
        }
    }

    @After
    public void after() throws SQLException {
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("clear ccl_triggers");
            stmt.execute("clear ccl_rules");
            stmt.execute(" /*TDDL:FORBID_EXECUTE_DML_ALL=FALSE*/delete from  " + CCL_TEST_TABLE_NAME);
        }

    }

    @Test
    public void test1a() throws SQLException {
        try (Statement stmt = tddlConnection.createStatement()) {
            String createSql =
                "CREATE CCL_RULE  if not exists testassignments ON `*`.`*` TO 'polardbx_root'@'%' " + "FOR SELECT "
                    + "FILTER BY KEYWORD('" + keyword + "') "
                    + "WITH MAX_CONCURRENCY=1,WAIT_TIMEOUT=11,WAIT_QUEUE_SIZE=11,FAST_MATCH=1";
            try {
                stmt.execute(createSql);
            } catch (Exception e) {
                e.printStackTrace();
            }

            try (ResultSet rs = stmt.executeQuery("show ccl_rule testassignments")) {
                rs.next();
                Assert.assertTrue(rs.getInt("WAIT_QUEUE_SIZE_PER_NODE") == 11);
                Assert.assertTrue(rs.getInt("FAST_MATCH") == 1);
                Assert.assertTrue(rs.getInt("MAX_CONCURRENCY_PER_NODE") == 1);
                Assert.assertTrue(rs.getInt("WAIT_TIMEOUT") == 11);
            }

        }

    }

    @Test
    public void test1() throws SQLException {
        try (Statement stmt = tddlConnection.createStatement()) {

            try (ResultSet rs = stmt.executeQuery("show ccl_rule busu1118")) {
                rs.next();
                Assert.assertEquals("busu1118", rs.getString("RULE_NAME"));
                Assert.assertEquals("SELECT", rs.getString("SQL_TYPE"));
                Assert.assertEquals(userName + "@%", rs.getString("USER"));
                Assert.assertEquals("*.*", rs.getString("TABLE"));
                Assert.assertTrue(rs.getInt("MAX_CONCURRENCY_PER_NODE") == 0);
                Assert.assertEquals("[\"" + keyword + "\"]", rs.getString("KEYWORDS"));
                Assert.assertEquals(null, rs.getString("TEMPLATE_ID"));
                Assert.assertTrue(rs.getInt("WAIT_QUEUE_SIZE_PER_NODE") == 0);
                Assert.assertTrue(rs.getInt("RUNNING") == 0);
                Assert.assertTrue(rs.getInt("WAITING") == 0);
                Assert.assertTrue(rs.getInt("KILLED") == 0);
                Assert.assertTrue(rs.getInt("ACTIVE_NODE_COUNT") > 0);
            }

            try (ResultSet rs = stmt.executeQuery("show ccl_rules")) {
                rs.next();
                Assert.assertEquals("busu1118", rs.getString("RULE_NAME"));
            }

            try (ResultSet rs = stmt.executeQuery("show ccl_rules")) {
                rs.next();
                Assert.assertEquals("busu1118", rs.getString("RULE_NAME"));
            }

            SQLException exception = null;
            try (ResultSet rs = stmt.executeQuery(String.format("select 1 as %s", keyword))) {

            } catch (SQLException e) {
                exception = e;
            }
            Assert.assertTrue(exception != null);
            Assert.assertTrue(exception.getMessage().contains("busu1118"));

            try (ResultSet rs = stmt.executeQuery("show ccl_rules")) {
                Assert.assertTrue(rs.next());
                Assert.assertTrue(rs.getInt("KILLED") == 1);
            }

        }
    }

    @Test
    public void test2() throws SQLException {
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("drop ccl_rule busu1118");

            try (ResultSet rs = stmt.executeQuery("show ccl_rules")) {
                Assert.assertFalse(rs.next());
            }

        }

    }

    @Test
    public void test3() throws SQLException {
        String[] cclRules = {
            "CREATE CCL_RULE  if not exists busu1118 ON `*`.`*` TO '" + userName + "'@'%'\n" + "FOR SELECT \n"
                + " FILTER BY KEYWORD('sleep')\n"
                + " WITH MAX_CONCURRENCY=0,WAIT_QUEUE_SIZE=1,LIGHT_WAIT=0",
            "CREATE CCL_RULE  if not exists busu1118 ON `*`.`*` TO '" + userName + "'@'%'\n" + "FOR SELECT \n"
                + " FILTER BY KEYWORD('sleep')\n"
                + " WITH MAX_CONCURRENCY=0,WAIT_QUEUE_SIZE=1,LIGHT_WAIT=1"};
        for (String cclRule : cclRules) {
            try (Statement stmt = tddlConnection.createStatement()) {
                stmt.execute("clear ccl_rules;");
                stmt.execute(cclRule);
                final Connection anotherConnection = getPolardbxDirectConnection();
                Thread thread = new Thread() {
                    @SneakyThrows
                    public void run() {

                        try (Connection connection = anotherConnection; Statement stmt = connection.createStatement()) {
                            stmt.execute("select sleep(6)");
                        } catch (Exception e) {
                            System.out.println(ExceptionUtils.getFullStackTrace(e));
                        }
                    }
                };
                thread.start();
                Thread.sleep(1000);
                try (ResultSet rs = stmt.executeQuery("show full processlist")) {
                    boolean hasWait = false;
                    while (rs.next()) {
                        if (rs.getString("Command").contains("busu1118")) {
                            hasWait = true;
                        }
                        if (hasWait) {
                            break;
                        }
                    }
                    Assert.assertTrue(hasWait);
                }
                try (ResultSet rs = stmt.executeQuery("show ccl_rules")) {
                    boolean hasWait = false;
                    while (rs.next()) {
                        if (rs.getInt("Waiting") > 0) {
                            hasWait = true;
                            break;
                        }
                    }
                    Assert.assertTrue(hasWait);
                }
                anotherConnection.abort(Executors.newCachedThreadPool());
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testCclWaitTimeout() throws SQLException {
        String[] cclRules = {
            "CREATE CCL_RULE  if not exists busu1118 ON `*`.`*` TO '" + userName + "'@'%'\n" + "FOR SELECT \n"
                + " FILTER BY KEYWORD('sleep')\n"
                + " WITH MAX_CONCURRENCY=0,WAIT_QUEUE_SIZE=1,LIGHT_WAIT=0,WAIT_TIMEOUT=6",
            "CREATE CCL_RULE  if not exists busu1118 ON `*`.`*` TO '" + userName + "'@'%'\n" + "FOR SELECT \n"
                + " FILTER BY KEYWORD('sleep')\n"
                + " WITH MAX_CONCURRENCY=0,WAIT_QUEUE_SIZE=1,LIGHT_WAIT=1,WAIT_TIMEOUT=6"};
        for (String cclRule : cclRules) {
            try (Statement stmt = tddlConnection.createStatement()) {
                stmt.execute("clear ccl_rules;");
                stmt.execute(cclRule);
                final Connection anotherConnection = getPolardbxDirectConnection();
                Thread thread = new Thread() {
                    @SneakyThrows
                    public void run() {

                        try (Connection connection = anotherConnection; Statement stmt = connection.createStatement()) {
                            stmt.execute("select sleep(6)");
                        } catch (Exception e) {
                            System.out.println(ExceptionUtils.getFullStackTrace(e));
                        }
                    }
                };
                thread.start();
                Thread.sleep(1000);
                try (ResultSet rs = stmt.executeQuery("show full processlist")) {
                    boolean hasWait = false;
                    while (rs.next()) {
                        if (rs.getString("Command").contains("busu1118")) {
                            hasWait = true;
                        }
                        if (hasWait) {
                            break;
                        }
                    }
                    Assert.assertTrue(hasWait);
                }
                try (ResultSet rs = stmt.executeQuery("show ccl_rules")) {
                    boolean hasWait = false;
                    while (rs.next()) {
                        if (rs.getInt("Waiting") != 0) {
                            hasWait = true;
                            break;
                        }
                    }
                    Assert.assertTrue(hasWait);
                }
                Thread.sleep(8000);
                try (ResultSet rs = stmt.executeQuery("show full processlist")) {
                    boolean hasWait = false;
                    while (rs.next()) {
                        if (rs.getString("Command").contains("busu1118")) {
                            hasWait = true;
                        }
                        if (hasWait) {
                            break;
                        }
                    }
                    Assert.assertTrue(!hasWait);
                }
                try (ResultSet rs = stmt.executeQuery("show ccl_rules")) {
                    boolean hasWait = false;
                    while (rs.next()) {
                        if (rs.getInt("Waiting") != 0) {
                            hasWait = true;
                            break;
                        }
                    }
                    Assert.assertTrue(!hasWait);
                }
                ExecutorService executorService = Executors.newCachedThreadPool();
                anotherConnection.abort(executorService);
                thread.join();
                executorService.shutdown();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testRunWaitTimeout() throws SQLException {
        String[] cclRules = {
            "CREATE CCL_RULE  if not exists busu1118 ON `*`.`*` TO '" + userName + "'@'%'\n" + "FOR SELECT \n"
                + " FILTER BY KEYWORD('sleep')\n"
                + " WITH MAX_CONCURRENCY=1,WAIT_QUEUE_SIZE=2,LIGHT_WAIT=0,WAIT_TIMEOUT=7",
            "CREATE CCL_RULE  if not exists busu1118 ON `*`.`*` TO '" + userName + "'@'%'\n" + "FOR SELECT \n"
                + " FILTER BY KEYWORD('sleep')\n"
                + " WITH MAX_CONCURRENCY=1,WAIT_QUEUE_SIZE=2,LIGHT_WAIT=1,WAIT_TIMEOUT=7"};
        for (String cclRule : cclRules) {
            try (Statement stmt = tddlConnection.createStatement()) {
                stmt.execute("clear ccl_rules;");
                stmt.execute(cclRule);
                List<Thread> threads = Lists.newArrayList();
                List<Connection> connections = Lists.newArrayList();
                Map<Integer, String> result = Maps.newConcurrentMap();
                for (int i = 0; i < 7; i++) {
                    Connection anotherConnection = getPolardbxDirectConnection();
                    connections.add(anotherConnection);
                    int finalI = i;
                    Thread thread = new Thread() {
                        @SneakyThrows
                        public void run() {
                            try (Connection connection = anotherConnection;
                                Statement stmt = connection.createStatement()) {
                                stmt.execute("select sleep(5)");
                                result.put(finalI + 10000, "finish");
                            } catch (Exception e) {
                                e.printStackTrace();
                                result.put(finalI, e.getMessage());
                            }
                        }
                    };
                    thread.start();
                    threads.add(thread);
                }
                Thread.sleep(3000);
                try (ResultSet rs = stmt.executeQuery("show full processlist")) {
                    boolean hasWait = false;
                    while (rs.next()) {
                        if (rs.getString("Command").contains("busu1118")) {
                            hasWait = true;
                        }
                    }
                    Assert.assertTrue(hasWait);
                }
                try (ResultSet rs = stmt.executeQuery("show ccl_rules")) {
                    boolean hasWait = false;
                    boolean hasRunning = false;
                    while (rs.next()) {
                        if (rs.getInt("Waiting") != 0) {
                            hasWait = true;
                        }
                        if (rs.getInt("Running") != 0) {
                            hasRunning = true;
                        }
                    }
                    Assert.assertTrue(hasWait);
                    Assert.assertTrue(hasRunning);
                }
                Thread.sleep(12000);
                try (ResultSet rs = stmt.executeQuery("show ccl_rules")) {
                    boolean hasWait = false;
                    boolean hasRunning = false;
                    while (rs.next()) {
                        if (rs.getInt("Waiting") != 0) {
                            hasWait = true;
                        }
                        if (rs.getInt("Running") != 0) {
                            hasRunning = true;
                        }
                    }
                    Assert.assertTrue(!hasWait);
                    Assert.assertTrue(!hasRunning);
                }
                System.out.println("result------" + JSON.toJSONString(result));
                Assert.assertEquals(7, result.size());
                ExecutorService executorService = Executors.newCachedThreadPool();
                for (Connection connection : connections) {
                    try {
                        connection.abort(executorService);
                    } catch (Throwable throwable) {
                        throwable.printStackTrace();
                    }
                }
                for (Thread thread : threads) {
                    thread.join();
                }
                executorService.shutdown();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    public void testPrepareMatch() {
        Exception exception = null;
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("clear ccl_rules;");
            stmt.execute(
                "CREATE CCL_RULE  if not exists busu1118 ON `*`.`*` TO '" + userName + "'@'%'\n" + "FOR SELECT \n"
                    + " FILTER BY KEYWORD(\"dingfeng\")\n" + " WITH MAX_CONCURRENCY=0");

            try (PreparedStatement pStmt = tddlConnection.prepareStatement("select ?")) {
                pStmt.setString(1, "dingfeng");
                pStmt.executeQuery();
            }

        } catch (Exception e) {
            exception = e;
        }

        Assert.assertTrue(exception != null);
        Assert.assertTrue(exception.getMessage().contains("busu1118"));

    }

    @Test
    public void testNoMatchCache() {
        Exception exception = null;
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("clear ccl_rules;");
            stmt.execute(
                "CREATE CCL_RULE  if not exists busu1118 ON `*`.`*` TO '" + userName + "'@'%'\n" + "FOR SELECT \n"
                    + " FILTER BY KEYWORD('dingfeng')\n" + " WITH MAX_CONCURRENCY=0");
            try (PreparedStatement pStmt = tddlConnection.prepareStatement(
                "update " + CCL_TEST_TABLE_NAME + " set myname = ? where id = 1")) {
                pStmt.setString(1, "busu");
                pStmt.executeUpdate();
                pStmt.setString(1, "dingfeng");
                pStmt.executeUpdate();
            }

        } catch (Exception e) {
            exception = e;
        }
        Assert.assertTrue(exception == null);
    }

    @Test
    public void testCclFilterByNoUser() throws Exception {
        Exception exception = null;
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("clear ccl_rules;");
            stmt.execute("CREATE CCL_RULE  if not exists busu1118 ON " + "*.*  " + "FOR SELECT "
                + " FILTER BY QUERY 'select * from " + CCL_TEST_TABLE_NAME + " where id = ?'\n"
                + " WITH MAX_CONCURRENCY=0");
            for (int i = 0; i < 5; ++i) {
                try {
                    stmt.execute("select * from " + CCL_TEST_TABLE_NAME + " where id = 1");
                } catch (Exception e) {
                    exception = e;
                }
                Thread.sleep(200);
            }
        }
        Assert.assertTrue(exception != null);
        Assert.assertTrue(exception.getMessage().contains("Exceeding the max concurrency"));
    }

    @Test
    public void testExplainContainingTemplateId() throws SQLException {
        try (Statement stmt = tddlConnection.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("explain select 1 as a")) {
                boolean hasTemplateId = false;
                while (rs.next()) {
                    String row = rs.getString(1);
                    if (row.contains("TemplateId")) {
//                        System.out.println(row);
                        hasTemplateId = true;
                    }
                }
                Assert.assertTrue(hasTemplateId);
            }

        }
    }

    @Test
    public void testCclFilterByQueryTemplate() throws Exception {
        Exception exception = null;
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("clear ccl_rules;");
            stmt.execute(
                "CREATE CCL_RULE  if not exists busu1118 ON " + "*.* TO '" + userName + "'@'%'\n" + "FOR SELECT \n"
                    + " FILTER BY QUERY 'select * from " + CCL_TEST_TABLE_NAME + " where id = ?'\n"
                    + " WITH MAX_CONCURRENCY=0");
            for (int i = 0; i < 5; ++i) {
                try {
                    stmt.execute("select * from " + CCL_TEST_TABLE_NAME + " where id = 1");
                } catch (Exception e) {
                    exception = e;
                }
                Thread.sleep(200);
            }
        }
        Assert.assertTrue(exception != null);
        Assert.assertTrue(exception.getMessage().contains("Exceeding the max concurrency"));
    }

    @Test
    public void testCclFilterByCompleteQuery() throws Exception {
        Exception exception = null;
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("clear ccl_rules;");
            String database = testNoMatchCacheDbName;
            stmt.execute("use " + database);
            stmt.execute("CREATE CCL_RULE  if not exists busu1118 ON " + database + ".`*` TO '" + userName + "'@'%'\n"
                + "FOR SELECT \n" + " FILTER BY QUERY 'select * from " + CCL_TEST_TABLE_NAME + " where id = 1'\n"
                + " WITH MAX_CONCURRENCY=0");
            for (int i = 0; i < 5; ++i) {
                try {
                    stmt.execute("select * from " + CCL_TEST_TABLE_NAME + " where id = 1");
                } catch (Exception e) {
                    exception = e;
                }
                Thread.sleep(200);
            }
            Assert.assertTrue(exception != null);
            Assert.assertTrue(exception.getMessage().contains("Exceeding the max concurrency"));
            stmt.execute("select * from " + CCL_TEST_TABLE_NAME + " where id = 2");
            exception = null;
            for (int i = 0; i < 5; ++i) {
                try {
                    stmt.execute("select * from " + CCL_TEST_TABLE_NAME + " where id = 1");
                } catch (Exception e) {
                    exception = e;
                }
                Thread.sleep(200);
            }
            Assert.assertTrue(exception != null);
            Assert.assertTrue(exception.getMessage().contains("Exceeding the max concurrency"));
        }
    }

    @Test
    public void testCclFilterBySemiQuery() throws Exception {
        Exception exception = null;
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("clear ccl_rules;");
            String database = testNoMatchCacheDbName;
            stmt.execute("use " + database);
            stmt.execute("CREATE CCL_RULE  if not exists busu1118 ON " + database + ".`*` TO '" + userName + "'@'%'\n"
                + "FOR SELECT \n" + " FILTER BY QUERY 'select * from " + CCL_TEST_TABLE_NAME
                + " where id = 1 and myname = ?'\n" + " WITH MAX_CONCURRENCY=0");
            for (int i = 0; i < 5; ++i) {
                try {
                    stmt.execute("select * from " + CCL_TEST_TABLE_NAME + " where id = 1 and myname = 'busu'");
                } catch (Exception e) {
                    exception = e;
                }
                Thread.sleep(200);
            }
            Assert.assertTrue(exception != null);
            Assert.assertTrue(exception.getMessage().contains("Exceeding the max concurrency"));
            stmt.execute("select * from " + CCL_TEST_TABLE_NAME + " where id = 2 and myname = 'busu'");
            exception = null;
            for (int i = 0; i < 5; ++i) {
                try {
                    stmt.execute("select * from " + CCL_TEST_TABLE_NAME + " where id = 1 and myname = 'busu'");
                } catch (Exception e) {
                    exception = e;
                }
                Thread.sleep(200);
            }
            Assert.assertTrue(exception != null);
            Assert.assertTrue(exception.getMessage().contains("Exceeding the max concurrency"));
        }
    }

    @Test
    public void testTableUnquoteKeyword() throws Exception {
        Exception exception = null;
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("clear ccl_rules;");
            String database = testNoMatchCacheDbName;
            stmt.execute("use " + database);
            stmt.execute(
                "CREATE CCL_RULE  if not exists busu1118 ON " + database + ".`" + CCL_TEST_TABLE_NAME + "` TO '"
                    + userName + "'@'%'\n" + "FOR SELECT \n" + " FILTER BY KEYWORD('" + keyword + "') "
                    + " WITH MAX_CONCURRENCY=0");

            for (int i = 0; i < 50; ++i) {
                try {
                    stmt.execute(
                        "select * from `" + CCL_TEST_TABLE_NAME + "` where id = 1 and myname = '" + keyword + "'");
                } catch (Exception e) {
                    e.printStackTrace();
                    exception = e;
                    break;
                }
                Thread.sleep(200);
            }
            Assert.assertTrue(exception != null);
            Assert.assertTrue(exception.getMessage().contains("Exceeding the max concurrency"));
        }
    }

}
