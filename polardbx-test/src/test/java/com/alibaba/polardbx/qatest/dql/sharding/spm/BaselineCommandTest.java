package com.alibaba.polardbx.qatest.dql.sharding.spm;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.repo.mysql.handler.LogicalBaselineHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author fangwu
 */
public class BaselineCommandTest extends BaseTestCase {
    private static final String db = "BaselineCommandTest";
    private static final String table = "BaselineCommandTest";
    private static final String table1 = "BaselineCommandTest1";
    private static final String createTbl = "CREATE TABLE `%s` (\n"
        + "`id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',\n"
        + "`creator` varchar(64) NOT NULL DEFAULT '' ,\n"
        + "`extend` varchar(128) NOT NULL DEFAULT '' ,\n"
        + "PRIMARY KEY (`id`) "
        + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 ";

    private static final String createTbl1 = "CREATE TABLE `%s` (\n"
        + "`id` bigint(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',\n"
        + "`creator` varchar(64) NOT NULL DEFAULT '' ,\n"
        + "`extend` varchar(128) NOT NULL DEFAULT '' ,\n"
        + "PRIMARY KEY (`id`) "
        + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 dbpartition by hash(id) tbpartition by hash(id) tbpartitions 3;";

    private static final String testSqlTemp = "baseline add sql /*TDDL:a()*/ select * from %s ";

    @Before
    public void buildCatalog() throws SQLException {
        try (Connection c = getPolardbxConnection()) {
            c.createStatement().execute("drop database if exists " + db);
            c.createStatement().execute("create database if not exists " + db + " mode='drds'");
            c.createStatement().execute("use " + db);
            c.createStatement().execute(String.format(createTbl, table));
            c.createStatement().execute(String.format(createTbl1, table1));
        }
    }

    @After
    public void clean() throws SQLException {
        try (Connection c = getPolardbxConnection()) {
            c.createStatement().execute("drop database if exists " + db);
        }
    }

    @Test
    public void testAddPlanThatBaselineNotSupported() {
        try (Connection c = getPolardbxConnection(db)) {
            String testSql = String.format(testSqlTemp, table);
            // test plan in plan cache
            c.createStatement().execute(testSql);
            Assert.fail();
        } catch (SQLException e) {
            if (e.getErrorCode() != 7001) {
                Assert.fail("not expected baseline error");
            }
        }
    }

    @Test
    public void testBaselineHelp() throws SQLException {
        String testSql = "baseline help";
        try (Connection c = getPolardbxConnection(db)) {
            ResultSet rs = c.createStatement().executeQuery(testSql);
            while (rs.next()) {
                String statement = rs.getString("STATEMENT");
                String desc = rs.getString("DESCRIPTION");
                String example = rs.getString("EXAMPLE");
                LogicalBaselineHandler.BASELINE_OPERATION operation =
                    LogicalBaselineHandler.BASELINE_OPERATION.valueOf(statement);
                Assert.assertTrue(desc.equalsIgnoreCase(operation.getDesc()));
                Assert.assertTrue(example.equalsIgnoreCase(operation.getExample()));
            }
        }
    }

    @Test
    public void testBaselineAddListDelete() throws SQLException {
        int baselineId1;
        int baselineId2;
        int baselineId3;
        try (Connection c = getPolardbxConnection(db)) {
            // add plan
            String baselineAdd = "baseline add sql /*TDDL:a()*/ select * from %s t1 join %s t2 on t1.id = t2.id";
            ResultSet resultSet = c.createStatement().executeQuery(String.format(baselineAdd, table, table1));
            resultSet.next();
            baselineId1 = resultSet.getInt("BASELINE_ID");
            resultSet.close();

            // fix plan
            baselineAdd = "baseline fix sql /*TDDL:b()*/ select * from %s t1 join %s t2 on t1.id = t2.id limit 1";
            resultSet = c.createStatement().executeQuery(String.format(baselineAdd, table, table1));
            resultSet.next();
            baselineId2 = resultSet.getInt("BASELINE_ID");
            resultSet.close();

            // hint bind
            baselineAdd =
                "baseline hint bind /*TDDL:c()*/ select * from %s t1 join %s t2 on t1.id = t2.id order by t1.creator limit 10";
            resultSet = c.createStatement().executeQuery(String.format(baselineAdd, table, table1));
            resultSet.next();
            baselineId3 = resultSet.getInt("BASELINE_ID");
            resultSet.close();
        }

        // check baseline list
        try (Connection c1 = getPolardbxConnection(db)) {
            ResultSet rs = c1.createStatement().executeQuery("baseline list");
            while (rs.next()) {
                int testBaselineId = rs.getInt("BASELINE_ID");
                String fixed = rs.getString("FIXED");
                String hint = rs.getString("HINT");
                String rebuild = rs.getString("IS_REBUILD_AT_LOAD");
                if (testBaselineId == baselineId1) {
                    Assert.assertTrue(fixed.equalsIgnoreCase("0"));
                    Assert.assertTrue(hint.equalsIgnoreCase("/*TDDL:a()*/"));
                    Assert.assertTrue(rebuild.equalsIgnoreCase("false"));
                } else if (testBaselineId == baselineId2) {
                    Assert.assertTrue(fixed.equalsIgnoreCase("1"));
                    Assert.assertTrue(hint.equalsIgnoreCase("/*TDDL:b()*/"));
                    Assert.assertTrue(rebuild.equalsIgnoreCase("false"));
                } else if (testBaselineId == baselineId3) {
                    Assert.assertTrue(fixed.equalsIgnoreCase("1"));
                    Assert.assertTrue(hint.equalsIgnoreCase("/*TDDL:c()*/"));
                    Assert.assertTrue(rebuild.equalsIgnoreCase("true"));
                }
            }
        }

        // test delete
        try (Connection c2 = getPolardbxConnection(db)) {
            c2.createStatement().execute(String.format("baseline delete %d", baselineId1));
            c2.createStatement().execute(String.format("baseline delete %d", baselineId2));
            c2.createStatement().execute(String.format("baseline delete %d", baselineId3));

            try (Connection c3 = getPolardbxConnection(db)) {
                ResultSet rs = c3.createStatement().executeQuery("baseline list");
                while (rs.next()) {
                    int testBaselineId = rs.getInt("BASELINE_ID");
                    Assert.assertTrue(testBaselineId != baselineId1 && testBaselineId != baselineId2
                        && testBaselineId != baselineId3);
                }
            }
        }
    }

    @Test
    public void testDeleteAll() throws SQLException {
        // add plan
        try (Connection c = getPolardbxConnection(db)) {
            // add plan
            String baselineAdd = "baseline add sql /*TDDL:a()*/ select * from %s t1 join %s t2 on t1.id = t2.id";
            c.createStatement().executeQuery(String.format(baselineAdd, table, table1));

            // fix plan
            baselineAdd = "baseline fix sql /*TDDL:b()*/ select * from %s t1 join %s t2 on t1.id = t2.id limit 1";
            c.createStatement().executeQuery(String.format(baselineAdd, table, table1));

            // hint bind
            baselineAdd =
                "baseline hint bind /*TDDL:c()*/ select * from %s t1 join %s t2 on t1.id = t2.id order by t1.creator limit 10";
            c.createStatement().executeQuery(String.format(baselineAdd, table, table1));
        }
        // test delete
        try (Connection c2 = getPolardbxConnection(db)) {
            // delete all
            c2.createStatement().execute("baseline delete_all");
            try (Connection c3 = getPolardbxConnection(db)) {
                ResultSet rs = c3.createStatement().executeQuery("baseline list");
                while (rs.next()) {
                    Assert.fail();
                }
            }
        }
    }

    @Test
    public void testDeletePlan() throws SQLException {
        int baselineId1;
        int baselineId2;
        int baselineId3;
        int planId1;
        int planId2;
        Integer planId3;
        try (Connection c = getPolardbxConnection(db)) {
            // add plan
            String baselineAdd = "baseline add sql /*TDDL:a()*/ select * from %s t1 join %s t2 on t1.id = t2.id";
            ResultSet resultSet = c.createStatement().executeQuery(String.format(baselineAdd, table, table1));
            resultSet.next();
            baselineId1 = resultSet.getInt("BASELINE_ID");
            planId1 = resultSet.getInt("PLAN_ID");
            resultSet.close();

            // fix plan
            baselineAdd = "baseline fix sql /*TDDL:b()*/ select * from %s t1 join %s t2 on t1.id = t2.id limit 1";
            resultSet = c.createStatement().executeQuery(String.format(baselineAdd, table, table1));
            resultSet.next();
            baselineId2 = resultSet.getInt("BASELINE_ID");
            planId2 = resultSet.getInt("PLAN_ID");
            resultSet.close();

            // hint bind
            baselineAdd =
                "baseline hint bind /*TDDL:c()*/ select * from %s t1 join %s t2 on t1.id = t2.id order by t1.creator limit 10";
            resultSet = c.createStatement().executeQuery(String.format(baselineAdd, table, table1));
            resultSet.next();
            baselineId3 = resultSet.getInt("BASELINE_ID");
            planId3 = resultSet.getInt("PLAN_ID");
            Assert.assertTrue(planId3 == 0);
            resultSet.close();
        }

        // check baseline list
        try (Connection c1 = getPolardbxConnection(db)) {
            ResultSet rs = c1.createStatement().executeQuery("baseline list");
            while (rs.next()) {
                int testBaselineId = rs.getInt("BASELINE_ID");
                String fixed = rs.getString("FIXED");
                String hint = rs.getString("HINT");
                String rebuild = rs.getString("IS_REBUILD_AT_LOAD");
                if (testBaselineId == baselineId1) {
                    Assert.assertTrue(fixed.equalsIgnoreCase("0"));
                    Assert.assertTrue(hint.equalsIgnoreCase("/*TDDL:a()*/"));
                    Assert.assertTrue(rebuild.equalsIgnoreCase("false"));
                } else if (testBaselineId == baselineId2) {
                    Assert.assertTrue(fixed.equalsIgnoreCase("1"));
                    Assert.assertTrue(hint.equalsIgnoreCase("/*TDDL:b()*/"));
                    Assert.assertTrue(rebuild.equalsIgnoreCase("false"));
                } else if (testBaselineId == baselineId3) {
                    Assert.assertTrue(fixed.equalsIgnoreCase("1"));
                    Assert.assertTrue(hint.equalsIgnoreCase("/*TDDL:c()*/"));
                    Assert.assertTrue(rebuild.equalsIgnoreCase("true"));
                }
            }
        }

        // test delete
        try (Connection c2 = getPolardbxConnection(db)) {
            c2.createStatement().execute(String.format("baseline delete_plan %d", planId1));
            c2.createStatement().execute(String.format("baseline delete_plan %d", planId2));
            c2.createStatement().execute(String.format("baseline delete %d", baselineId3));

            try (Connection c3 = getPolardbxConnection(db)) {
                ResultSet rs = c3.createStatement().executeQuery("baseline list");
                while (rs.next()) {
                    int testBaselineId = rs.getInt("BASELINE_ID");
                    Assert.assertTrue(testBaselineId != baselineId1 && testBaselineId != baselineId2
                        && testBaselineId != baselineId3);
                }
            }
        }
    }
}