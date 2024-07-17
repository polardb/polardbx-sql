package com.alibaba.polardbx.qatest.twoPhaseDdl;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil.checkIfExecuteTwoPhaseDdl;

@NotThreadSafe
@RunWith(Parameterized.class)
public class TwoPhaseDdlCheckApplicabilityTest extends DDLBaseNewDBTestCase {

    final static Log log = LogFactory.getLog(TwoPhaseDdlCheckApplicabilityTest.class);
    private String tableName = "";
    private static final String createOption = " if not exists ";

    public TwoPhaseDdlCheckApplicabilityTest(boolean crossSchema) {
        this.crossSchema = crossSchema;
    }

    public int smallDelay = 1;

    public int largeDelay = 5;

    @Parameterized.Parameters(name = "{index}:crossSchema={0}")
    public static List<Object[]> initParameters() {
        return Arrays.asList(new Object[][] {
            {false}});
    }

    @Before
    public void init() {
        this.tableName = schemaPrefix + randomTableName("two_phase", 4);
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Test
    public void testAlterTableModifyColumn() throws SQLException {
        String mytable = schemaPrefix + randomTableName("modify_column", 4);
        try {
            dropTableIfExists(mytable);
        } catch (Exception e) {
            log.info(e.getMessage());
        }
        String createTableStmt = "create table " + createOption + " %s(a int,b char, d int, primary key(d))";
        String sql = String.format(createTableStmt, mytable);
        log.info("execute sql: " + sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String enableTwoPhaseDdlHint = String.format("/*+TDDL:CMD_EXTRA(ENABLE_DRDS_MULTI_PHASE_DDL=true)*/");
        sql = String.format(
            enableTwoPhaseDdlHint + "ALTER TABLE %s MODIFY COLUMN a int",
            mytable);
        log.info("execute sql: " + sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertTrue("the results should contain two phase task!", checkIfExecuteTwoPhaseDdl(tddlConnection, sql));
        log.info("expected succeed, and contain two phase task");

        sql = String.format(
            enableTwoPhaseDdlHint + "ALTER TABLE %s MODIFY COLUMN a int, ALGORITHM=INPLACE",
            mytable);
        log.info("execute sql: " + sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertTrue("the results should contain two phase task!", checkIfExecuteTwoPhaseDdl(tddlConnection, sql));
        log.info("expected succeed, and contain two phase task");

        sql = String.format(
            enableTwoPhaseDdlHint + "ALTER TABLE %s MODIFY COLUMN a int, ALGORITHM=COPY",
            mytable);
        log.info("execute sql: " + sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertTrue("the results should contain two phase task!", checkIfExecuteTwoPhaseDdl(tddlConnection, sql));
        log.info("expected succeed, and contain two phase task");

        sql = String.format(
            enableTwoPhaseDdlHint + "ALTER TABLE %s MODIFY COLUMN a bigint",
            mytable);
        log.info("execute sql: " + sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertTrue("the result should contain two phase task!", checkIfExecuteTwoPhaseDdl(tddlConnection, sql));
        log.info("expected succeed, and contain two phase task");

        sql = String.format(
            enableTwoPhaseDdlHint + "ALTER TABLE %s MODIFY COLUMN a bigint, ALGORITHM=COPY",
            mytable);
        log.info("execute sql: " + sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertTrue("the result should contain two phase task!", checkIfExecuteTwoPhaseDdl(tddlConnection, sql));
        log.info("expected succeed, and contain two phase task");

        sql = String.format(
            enableTwoPhaseDdlHint + "ALTER TABLE %s MODIFY COLUMN a int, ALGORITHM=COPY",
            mytable);
        log.info("execute sql: " + sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertTrue("the result should contain two phase task!", checkIfExecuteTwoPhaseDdl(tddlConnection, sql));
        log.info("expected succeed, and contain two phase task");

        String errMsg = "ALGORITHM=INPLACE is not supported";
        sql = String.format(
            enableTwoPhaseDdlHint + "ALTER TABLE %s MODIFY COLUMN a bigint, ALGORITHM=INPLACE",
            mytable);
        log.info("execute sql: " + sql);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, errMsg);
        log.info("expected failed, and truely failed for:  " + errMsg);
    }

    @Test
    public void testAlterTableAddColumnAndDropColumn() throws SQLException {
        String mytable = schemaPrefix + randomTableName("add_column", 4);
        try {
            dropTableIfExists(mytable);
        } catch (Exception e) {
            log.info(e.getMessage());
        }
        String createTableStmt = "create table " + createOption + " %s(a int,b char, d int, primary key(d))";
        String sql = String.format(createTableStmt, mytable);
        log.info("execute sql: " + sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String enableTwoPhaseDdlHint = String.format("/*+TDDL:CMD_EXTRA(ENABLE_DRDS_MULTI_PHASE_DDL=true)*/");
        String disableTwoPhaseDdlHint = String.format("/*+TDDL:CMD_EXTRA(ENABLE_DRDS_MULTI_PHASE_DDL=false)*/");
        String errMsg = "We don't support set specified algorithm under two phase ddl, you can use";
        sql = String.format(
            enableTwoPhaseDdlHint + "ALTER TABLE %s ADD COLUMN e int",
            mytable);
        log.info("execute sql: " + sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertTrue("the result should contain two phase task!", checkIfExecuteTwoPhaseDdl(tddlConnection, sql));
        log.info("expected succeed, and contain two phase task");

        sql = String.format(
            enableTwoPhaseDdlHint + "ALTER TABLE %s DROP COLUMN e, ALGORITHM=COPY",
            mytable);
        log.info("execute sql: " + sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertTrue("the result should contain no two phase task!",
            !checkIfExecuteTwoPhaseDdl(tddlConnection, sql));
        log.info("expected succeed, and DO NOT contain two phase task");

        sql = String.format(
            enableTwoPhaseDdlHint + "ALTER TABLE %s ADD COLUMN e int, ALGORITHM=COPY",
            mytable);
        log.info("execute sql: " + sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertTrue("the result should contain no two phase task!",
            !checkIfExecuteTwoPhaseDdl(tddlConnection, sql));
        log.info("expected succeed, and DO NOT contain two phase task");

        sql = String.format(
            disableTwoPhaseDdlHint + "ALTER TABLE %s DROP COLUMN e, ALGORITHM=COPY",
            mytable);
        log.info("execute sql: " + sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertTrue("the result should contain no two phase task!",
            !checkIfExecuteTwoPhaseDdl(tddlConnection, sql));
        log.info("expected succeed, and DO NOT contain two phase task");

        sql = String.format(
            enableTwoPhaseDdlHint + "ALTER TABLE %s ADD COLUMN f int, ALGORITHM=INPLACE",
            mytable);
        log.info("execute sql: " + sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertTrue("the result should contain two phase task!", checkIfExecuteTwoPhaseDdl(tddlConnection, sql));
        log.info("expected succeed, and contain two phase task");

        sql = String.format(
            enableTwoPhaseDdlHint + "ALTER TABLE %s DROP COLUMN a",
            mytable);
        log.info("execute sql: " + sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertTrue("the result should contain two phase task!", checkIfExecuteTwoPhaseDdl(tddlConnection, sql));
        log.info("expected succeed, and contain two phase task");
    }

    @Test
    public void testAlterTableMultiStatement() throws SQLException {
        String mytable = schemaPrefix + randomTableName("multi_statement", 4);
        try {
            dropTableIfExists(mytable);
        } catch (Exception e) {
            log.info(e.getMessage());
        }
        String createTableStmt = "create table " + createOption + " %s(a int,b char, d int, primary key(d))";
        String sql = String.format(createTableStmt, mytable);
        log.info("execute sql: " + sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String enableTwoPhaseDdlHint = String.format("/*+TDDL:CMD_EXTRA(ENABLE_DRDS_MULTI_PHASE_DDL=true)*/");
        sql = String.format(
            enableTwoPhaseDdlHint + "ALTER TABLE %s ADD COLUMN e int, ADD COLUMN f int",
            mytable);
        log.info("execute sql: " + sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertTrue("the result contains no two phase task!", !checkIfExecuteTwoPhaseDdl(tddlConnection, sql));
        log.info("expected succeed, and DO NOT contain two phase task");
    }

}
