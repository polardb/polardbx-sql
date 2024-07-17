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
public class TwoPhaseDdlSetSqlModeAndServerVariablesTest extends DDLBaseNewDBTestCase {

    final static Log log = LogFactory.getLog(TwoPhaseDdlSetSqlModeAndServerVariablesTest.class);
    private String tableName = "";
    private static final String createOption = " if not exists ";

    public TwoPhaseDdlSetSqlModeAndServerVariablesTest(boolean crossSchema) {
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
    public void testAlterTableModifyColumnNotNull() throws SQLException {
        String mytable = schemaPrefix + randomTableName("int_column_null", 4);
        try {
            dropTableIfExists(mytable);
        } catch (Exception e) {
            log.info(e.getMessage());
        }
        String createTableStmt = "create table " + createOption + " %s(a int DEFAULT NULL)";
        String sql = String.format(createTableStmt, mytable);
        log.info("execute sql: " + sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        log.info("execute sql: " + sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        String enableTwoPhaseDdlHint = String.format("/*+TDDL:CMD_EXTRA(ENABLE_DRDS_MULTI_PHASE_DDL=true)*/");
        sql = "set session sql_mode = 'STRICT_TRANS_TABLES'";
        log.info("execute sql: " + sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(
            enableTwoPhaseDdlHint + "ALTER TABLE %s MODIFY COLUMN a int NOT NULL, ALGORITHM=INPLACE;",
            mytable);
        log.info("execute sql: " + sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        log.info("expected succeed, and contain two phase task");

        try {
            dropTableIfExists(mytable);
        } catch (Exception e) {
            log.info(e.getMessage());
        }
        sql = String.format(createTableStmt, mytable);
        log.info("execute sql: " + sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = "set session sql_mode = ''";
        log.info("execute sql: " + sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = "set session sql_mode = ''";
        sql = String.format(
            enableTwoPhaseDdlHint + "ALTER TABLE %s MODIFY COLUMN a int NOT NULL, ALGORITHM=INPLACE",
            mytable);
        log.info("execute sql: " + sql);
        String errMsg = "ALGORITHM=INPLACE is not supported";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, errMsg);
        log.info("expected succeed, and contain two phase task");
    }

    @Test
    public void testAlterTableModifyColumnShrink() throws SQLException {
        String mytable = schemaPrefix + randomTableName("int_column", 4);
        try {
            dropTableIfExists(mytable);
        } catch (Exception e) {
            log.info(e.getMessage());
        }
        String createTableStmt = "create table " + createOption + " %s(a int DEFAULT NULL)";
        String insertTableStmt = "insert into table  %s(a) values (10000000);";
        String sql = String.format(createTableStmt, mytable);
        log.info("execute sql: " + sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(insertTableStmt, mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        log.info("execute sql: " + sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String enableTwoPhaseDdlHint = String.format("/*+TDDL:CMD_EXTRA(ENABLE_DRDS_MULTI_PHASE_DDL=true)*/");
        sql = "set sql_mode = ''";
        log.info("execute sql: " + sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(
            enableTwoPhaseDdlHint + "ALTER TABLE %s MODIFY COLUMN a smallint",
            mytable);
        log.info("execute sql: " + sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        Assert.assertTrue("the results should contain two phase task!", checkIfExecuteTwoPhaseDdl(tddlConnection, sql));
        log.info("expected succeed, and contain two phase task");

        try {
            dropTableIfExists(mytable);
        } catch (Exception e) {
            log.info(e.getMessage());
        }
        sql = String.format(createTableStmt, mytable);
        log.info("execute sql: " + sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(insertTableStmt, mytable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        log.info("execute sql: " + sql);
        sql = "set sql_mode = 'STRICT_TRANS_TABLES'";
        log.info("execute sql: " + sql);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format(
            enableTwoPhaseDdlHint + "ALTER TABLE %s MODIFY COLUMN a smallint",
            mytable);
        log.info("execute sql: " + sql);
        String errMsg = " Out of range value for column";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, errMsg);
        Assert.assertTrue("the results should contain two phase task!", checkIfExecuteTwoPhaseDdl(tddlConnection, sql));
        log.info("expected succeed, and contain two phase task");
    }
}
