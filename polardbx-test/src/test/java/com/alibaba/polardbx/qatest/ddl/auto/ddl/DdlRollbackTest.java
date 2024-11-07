package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.calcite.util.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.util.JdbcUtil.getTopology;

public class DdlRollbackTest extends DDLBaseNewDBTestCase {

    private static final String tableSchema = "_rollbackDb_";
    private static final String hint =
        "/*+TDDL:cmd_extra(TABLEGROUP_REORG_FINAL_TABLE_STATUS_DEBUG = 'READY_TO_PUBLIC')*/ ";

    @Test
    public void testMoveTableRollback() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + tableSchema);
        String tableName = generateRandomName(10);
        int i = 0;
        do {
            i++;
            String sql1 = String.format("drop table if exists %s", tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);

            sql1 = String.format("create table %s( a int) partition by key(a) partitions 2", tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
            //GROUP_NAME-TABLE_NAME-PARTITION_NAME-DN_ID
            final List<List<String>> result = getTopology(tddlConnection, tableName);
            Assert.assertTrue(result.size() == 2);
            Assert.assertTrue(result.get(0).size() == 4);
            Assert.assertTrue(result.get(1).size() == 4);

            Assert.assertTrue(!result.get(0).get(3).equalsIgnoreCase(result.get(1).get(3)));

            sql1 = String.format("alter table %s move partitions %s to '%s'", tableName, result.get(0).get(2),
                result.get(1).get(3));

            String ignoreErr =
                "The DDL job has been paused or cancelled. Please use SHOW DDL";
            Set<String> ignoreErrs = new HashSet<>();
            ignoreErrs.add(ignoreErr);
            JdbcUtil.executeUpdateSuccessIgnoreErr(tddlConnection, hint + sql1,
                ignoreErrs);
            Long jobId = getDDLJobId(tddlConnection);
            String rollbackDdl = "rollback ddl " + jobId;
            if (i == 1) {
                JdbcUtil.executeUpdateSuccess(tddlConnection, rollbackDdl);
            } else {
                sql1 = String.format("/*+TDDL:node('%s')*/  drop table %s", result.get(0).get(0), result.get(0).get(1));
                JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
                JdbcUtil.executeFailed(tddlConnection, rollbackDdl, "The DDL job has been paused or cancelled");
            }
        } while (i <= 1);
    }

    @Test
    public void testMoveTableGroupRollback() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + tableSchema);
        String tableName1 = generateRandomName(10);
        String tableName2 = generateRandomName(10);
        int i = 0;
        do {
            i++;
            String sql1 = String.format("drop table if exists %s", tableName1);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);

            sql1 = String.format("create table %s( a int) partition by key(a) partitions 2", tableName1);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
            sql1 = String.format("drop table if exists %s", tableName2);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);

            sql1 = String.format("create table %s( a int) partition by key(a) partitions 2", tableName2);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
            //GROUP_NAME-TABLE_NAME-PARTITION_NAME-DN_ID
            final List<List<String>> result = getTopology(tddlConnection, tableName2);
            Assert.assertTrue(result.size() == 2);
            Assert.assertTrue(result.get(0).size() == 4);
            Assert.assertTrue(result.get(1).size() == 4);

            Assert.assertTrue(!result.get(0).get(3).equalsIgnoreCase(result.get(1).get(3)));

            sql1 = String.format("alter tablegroup by table %s move partitions %s to '%s'", tableName1,
                result.get(0).get(2),
                result.get(1).get(3));

            String ignoreErr =
                "The DDL job has been paused or cancelled. Please use SHOW DDL";
            Set<String> ignoreErrs = new HashSet<>();
            ignoreErrs.add(ignoreErr);
            JdbcUtil.executeUpdateSuccessIgnoreErr(tddlConnection, hint + sql1,
                ignoreErrs);
            Long jobId = getDDLJobId(tddlConnection);
            String rollbackDdl = "rollback ddl " + jobId;
            if (i == 1) {
                JdbcUtil.executeUpdateSuccess(tddlConnection, rollbackDdl);
            } else {
                sql1 = String.format("/*+TDDL:node('%s')*/  drop table %s", result.get(0).get(0), result.get(0).get(1));
                JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
                JdbcUtil.executeFailed(tddlConnection, rollbackDdl, "The DDL job has been paused or cancelled");
            }
        } while (i <= 1);
    }

    @Test
    public void testSplitTableRollback() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + tableSchema);
        String tableName = generateRandomName(10);
        int i = 0;
        do {
            i++;
            String sql1 = String.format("drop table if exists %s", tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);

            sql1 = String.format("create table %s( a int) partition by key(a) partitions 2", tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
            //GROUP_NAME-TABLE_NAME-PARTITION_NAME-DN_ID
            final List<List<String>> result = getTopology(tddlConnection, tableName);
            Assert.assertTrue(result.size() == 2);
            Assert.assertTrue(result.get(0).size() == 4);
            Assert.assertTrue(result.get(1).size() == 4);

            Assert.assertTrue(!result.get(0).get(3).equalsIgnoreCase(result.get(1).get(3)));

            sql1 = String.format("alter table %s split partition %s", tableName, result.get(0).get(2));

            String ignoreErr =
                "The DDL job has been paused or cancelled. Please use SHOW DDL";
            Set<String> ignoreErrs = new HashSet<>();
            ignoreErrs.add(ignoreErr);
            JdbcUtil.executeUpdateSuccessIgnoreErr(tddlConnection, hint + sql1,
                ignoreErrs);
            Long jobId = getDDLJobId(tddlConnection);
            String rollbackDdl = "rollback ddl " + jobId;
            if (i == 1) {
                JdbcUtil.executeUpdateSuccess(tddlConnection, rollbackDdl);
            } else {
                sql1 = String.format("/*+TDDL:node('%s')*/  drop table %s", result.get(0).get(0), result.get(0).get(1));
                JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
                JdbcUtil.executeFailed(tddlConnection, rollbackDdl, "The DDL job has been paused or cancelled");
            }
        } while (i <= 1);
    }

    @Test
    public void testSplitTableGroupRollback() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + tableSchema);
        String tableName1 = generateRandomName(10);
        String tableName2 = generateRandomName(10);
        int i = 0;
        do {
            i++;
            String sql1 = String.format("drop table if exists %s", tableName1);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);

            sql1 = String.format("create table %s( a int) partition by key(a) partitions 2", tableName1);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
            sql1 = String.format("drop table if exists %s", tableName2);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);

            sql1 = String.format("create table %s( a int) partition by key(a) partitions 2", tableName2);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
            //GROUP_NAME-TABLE_NAME-PARTITION_NAME-DN_ID
            final List<List<String>> result = getTopology(tddlConnection, tableName2);
            Assert.assertTrue(result.size() == 2);
            Assert.assertTrue(result.get(0).size() == 4);
            Assert.assertTrue(result.get(1).size() == 4);

            Assert.assertTrue(!result.get(0).get(3).equalsIgnoreCase(result.get(1).get(3)));

            sql1 = String.format("alter tablegroup by table %s split partition %s", tableName1, result.get(0).get(2));

            String ignoreErr =
                "The DDL job has been paused or cancelled. Please use SHOW DDL";
            Set<String> ignoreErrs = new HashSet<>();
            ignoreErrs.add(ignoreErr);
            JdbcUtil.executeUpdateSuccessIgnoreErr(tddlConnection, hint + sql1,
                ignoreErrs);
            Long jobId = getDDLJobId(tddlConnection);
            String rollbackDdl = "rollback ddl " + jobId;
            if (i == 1) {
                JdbcUtil.executeUpdateSuccess(tddlConnection, rollbackDdl);
            } else {
                sql1 = String.format("/*+TDDL:node('%s')*/  drop table %s", result.get(0).get(0), result.get(0).get(1));
                JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
                JdbcUtil.executeFailed(tddlConnection, rollbackDdl, "The DDL job has been paused or cancelled");
            }
        } while (i <= 1);
    }

    @Test
    public void testMergeTableRollback() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + tableSchema);
        String tableName = generateRandomName(10);
        int i = 0;
        do {
            i++;
            String sql1 = String.format("drop table if exists %s", tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);

            sql1 = String.format("create table %s( a int) partition by key(a) partitions 2", tableName);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
            //GROUP_NAME-TABLE_NAME-PARTITION_NAME-DN_ID
            final List<List<String>> result = getTopology(tddlConnection, tableName);
            Assert.assertTrue(result.size() == 2);
            Assert.assertTrue(result.get(0).size() == 4);
            Assert.assertTrue(result.get(1).size() == 4);

            Assert.assertTrue(!result.get(0).get(3).equalsIgnoreCase(result.get(1).get(3)));

            sql1 = String.format("alter table %s merge partitions %s, %s to %s", tableName, result.get(0).get(2),
                result.get(1).get(2), "pp");

            String ignoreErr =
                "The DDL job has been paused or cancelled. Please use SHOW DDL";
            Set<String> ignoreErrs = new HashSet<>();
            ignoreErrs.add(ignoreErr);
            JdbcUtil.executeUpdateSuccessIgnoreErr(tddlConnection, hint + sql1,
                ignoreErrs);
            Long jobId = getDDLJobId(tddlConnection);
            String rollbackDdl = "rollback ddl " + jobId;
            if (i == 1) {
                JdbcUtil.executeUpdateSuccess(tddlConnection, rollbackDdl);
            } else {
                sql1 = String.format("/*+TDDL:node('%s')*/  drop table %s", result.get(0).get(0), result.get(0).get(1));
                JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
                JdbcUtil.executeFailed(tddlConnection, rollbackDdl, "The DDL job has been paused or cancelled");
            }
        } while (i <= 1);
    }

    @Test
    public void testMergeTableGroupRollback() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + tableSchema);
        String tableName1 = generateRandomName(10);
        String tableName2 = generateRandomName(10);
        int i = 0;
        do {
            i++;
            String sql1 = String.format("drop table if exists %s", tableName1);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);

            sql1 = String.format("create table %s( a int) partition by key(a) partitions 2", tableName1);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
            sql1 = String.format("drop table if exists %s", tableName2);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);

            sql1 = String.format("create table %s( a int) partition by key(a) partitions 2", tableName2);
            JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
            //GROUP_NAME-TABLE_NAME-PARTITION_NAME-DN_ID
            final List<List<String>> result = getTopology(tddlConnection, tableName2);
            Assert.assertTrue(result.size() == 2);
            Assert.assertTrue(result.get(0).size() == 4);
            Assert.assertTrue(result.get(1).size() == 4);

            Assert.assertTrue(!result.get(0).get(3).equalsIgnoreCase(result.get(1).get(3)));

            sql1 =
                String.format("alter tablegroup by table %s merge partitions %s, %s to %s", tableName1,
                    result.get(0).get(2),
                    result.get(1).get(2), "pp");

            String ignoreErr =
                "The DDL job has been paused or cancelled. Please use SHOW DDL";
            Set<String> ignoreErrs = new HashSet<>();
            ignoreErrs.add(ignoreErr);
            JdbcUtil.executeUpdateSuccessIgnoreErr(tddlConnection, hint + sql1,
                ignoreErrs);
            Long jobId = getDDLJobId(tddlConnection);
            String rollbackDdl = "rollback ddl " + jobId;
            if (i == 1) {
                JdbcUtil.executeUpdateSuccess(tddlConnection, rollbackDdl);
            } else {
                sql1 = String.format("/*+TDDL:node('%s')*/  drop table %s", result.get(0).get(0), result.get(0).get(1));
                JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
                JdbcUtil.executeFailed(tddlConnection, rollbackDdl, "The DDL job has been paused or cancelled");
            }
        } while (i <= 1);
    }

    private static Long getDDLJobId(Connection connection) throws SQLException {
        long jobId = -1L;

        String sql = "show ddl";
        ResultSet rs = JdbcUtil.executeQuery(sql, connection);
        if (rs.next()) {
            jobId = rs.getLong("JOB_ID");
        }
        rs.close();
        return jobId;
    }

    public boolean usingNewPartDb() {
        return true;
    }

    public static String generateRandomName(int len) {
        String characters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
        Random random = new Random();

        return random.ints(len, 0, characters.length())
            .mapToObj(i -> characters.charAt(i))
            .map(Object::toString)
            .collect(Collectors.joining());
    }

    @Before
    public void setUp() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "use polardbx");
        String sql1 = String.format("drop database if exists %s", tableSchema);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);

        sql1 = String.format("create database %s mode=auto", tableSchema);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
    }

    @After
    public void cleanUp() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "use polardbx");
        String sql1 = String.format("drop database if exists %s", tableSchema);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql1);
    }

    public static List<List<String>> getTopology(Connection tddlConnection, String tableName) {
        //GROUP_NAME-TABLE_NAME-PARTITION_NAME-DN_ID
        final List<List<String>> result = new ArrayList<>();
        try (ResultSet topology = JdbcUtil.executeQuery("SHOW TOPOLOGY FROM `" + tableName + "`", tddlConnection)) {
            while (topology.next()) {
                List<String> row = new ArrayList<>();
                row.add(topology.getString(2));
                row.add(topology.getString(3));
                row.add(topology.getString(4));
                row.add(topology.getString(7));
                result.add(row);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Cannot get topology for " + tableName, e);
        }
        return result;
    }
}
