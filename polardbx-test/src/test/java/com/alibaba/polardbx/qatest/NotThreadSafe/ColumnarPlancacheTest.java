package com.alibaba.polardbx.qatest.NotThreadSafe;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.validator.DataValidator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

public class ColumnarPlancacheTest extends DDLBaseNewDBTestCase {
    private static String TABLE_DEFINITION_FORMAT = "CREATE TABLE `%s` (\n" +
        "\t`id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
        + "\t`a` int(32) UNSIGNED DEFAULT NULL,\n"
        + "\t`b` int(32) UNSIGNED DEFAULT NULL,\n"
        + "\t`c` int(32) UNSIGNED DEFAULT NULL,\n"
        + "\tprimary key(id)\n"
        + ") partition by key(id) partitions 8";

    private static String CREATE_COL_IDX = SKIP_WAIT_CCI_CREATION_HINT
        + "create clustered columnar index `%s` on %s(`%s`) partition by hash(`%s`) partitions 4";

    private static String PUB_COL_IDX =
        "/*+TDDL:CMD_EXTRA(ALTER_CCI_STATUS=true, ALTER_CCI_STATUS_BEFORE=CREATING, ALTER_CCI_STATUS_AFTER=PUBLIC)*/" +
            "ALTER TABLE `%s` alter index `%s` VISIBLE;";

    String tb1 = "tb1";

    String colIdxA = "colIdx_a";

    String colA = "a";

    String EMPTY_HINT = "/*+TDDL:cmd_extra()*/";
    String ENABLE_COL_CACHE = "set global " + ConnectionProperties.ENABLE_COLUMNAR_PLAN_CACHE + " = true";
    String DISABLE_COL_CACHE = "set global " + ConnectionProperties.ENABLE_COLUMNAR_PLAN_CACHE + " = false";

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Before
    public void prepareTable() {
        JdbcUtil.dropTable(getTddlConnection1(), tb1);
        JdbcUtil.executeSuccess(getTddlConnection1(), String.format(TABLE_DEFINITION_FORMAT, tb1));
        JdbcUtil.executeSuccess(getTddlConnection1(), String.format(CREATE_COL_IDX, colIdxA, tb1, colA, colA));
        JdbcUtil.executeSuccess(getTddlConnection1(), String.format(PUB_COL_IDX, tb1, colIdxA));
    }

    @After
    public void afterDDLBaseNewDBTestCase() {
        //cleanDataBase();
    }

    @Test
    public void testExplain() throws SQLException, InterruptedException {
        String sql1 = String.format("explain select id from %s where id < 1000+10", tb1);
        String sql2 = String.format("explain select id from %s where id > 1000+10", tb1);
        try (Connection conn1 = getPolardbxConnection();
            Connection conn2 = getPolardbxConnection()) {
            JdbcUtil.executeQuery("set WORKLOAD_TYPE=AP", conn1);
            JdbcUtil.executeQuery("set ENABLE_COLUMNAR_OPTIMIZER=true", conn1);
            JdbcUtil.executeQuery("set ENABLE_COLUMNAR_OPTIMIZER=false", conn2);

            JdbcUtil.executeQuery(DISABLE_COL_CACHE, conn1);
            Thread.sleep(3000L);
            String explain = DataValidator.getExplainAllResult(sql1, null, conn1);
            Assert.assertTrue(explain.contains("(`id` < 1010") && explain.contains("HitCache:false"));
            explain = DataValidator.getExplainAllResult(sql1, null, conn1);
            Assert.assertTrue(explain.contains("(`id` < 1010") && explain.contains("HitCache:false"));
            explain = DataValidator.getExplainAllResult(sql2, null, conn2);
            Assert.assertTrue(explain.contains("(`id` > (? + ?)") && explain.contains("HitCache:false"));
            explain = DataValidator.getExplainAllResult(sql2, null, conn2);
            Assert.assertTrue(explain.contains("(`id` > (? + ?)") && explain.contains("HitCache:true"));

            JdbcUtil.executeQuery(ENABLE_COL_CACHE, conn1);
            Thread.sleep(3000L);
            explain = DataValidator.getExplainAllResult(sql1, null, conn1);
            Assert.assertTrue(explain.contains("(`id` < (? + ?)") && explain.contains("HitCache:false"));
            explain = DataValidator.getExplainAllResult(sql1, null, conn1);
            Assert.assertTrue(explain.contains("(`id` < (? + ?)") && explain.contains("HitCache:true"));
            explain = DataValidator.getExplainAllResult(EMPTY_HINT + sql1, null, conn1);
            Assert.assertTrue(explain.contains("(`id` < 1010") && explain.contains("HitCache:false"));
            explain = DataValidator.getExplainAllResult(sql2, null, conn2);
            Assert.assertTrue(explain.contains("(`id` > (? + ?)") && explain.contains("HitCache:true"));

            JdbcUtil.executeQuery(DISABLE_COL_CACHE, conn1);
            Thread.sleep(3000L);
            explain = DataValidator.getExplainAllResult(sql1, null, conn1);
            Assert.assertTrue(explain.contains("(`id` < 1010") && explain.contains("HitCache:false"));
            explain = DataValidator.getExplainAllResult(sql1, null, conn1);
            Assert.assertTrue(explain.contains("(`id` < 1010") && explain.contains("HitCache:false"));

            explain = DataValidator.getExplainAllResult(sql2, null, conn2);
            Assert.assertTrue(explain.contains("(`id` > (? + ?)") && explain.contains("HitCache:true"));

        } finally {
            JdbcUtil.executeQuery(DISABLE_COL_CACHE, getTddlConnection1());
        }

    }
}
