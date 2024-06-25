package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.ReplaceRelocateWriter;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.planner.common.BasePlannerTest;
import org.apache.calcite.sql.SqlNode;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLSyntaxErrorException;

public class PushDownReplaceTest extends BasePlannerTest {
    private static FastsqlParser parser = new FastsqlParser();

    public PushDownReplaceTest() {
        super("test_db");
    }

    @Test
    public void testPushDownReplace() throws SQLSyntaxErrorException {
        prepareSchemaByDdl();
        String createTbDdl = "create table test_tb(id int auto_increment primary key, "
            + " a int, b int, c int, d int, data int, "
            + " clustered index gsi_t(c, d) dbpartition by hash (c, d), "
            + " local unique key uk(a, b, c, d)) dbpartition by hash(a, b)";
        buildTable(appName, createTbDdl);
        PlannerContext plannerContext = PlannerContext.fromExecutionContext(ec);
        plannerContext.setSchemaName("test_db");
        SqlNode ast = parser.parse("replace into test_tb (a, b, c, d, data) values (10, 10, 10, 10, 10)", ec).get(0);
        try {
            Planner planner = new Planner();
            ExecutionPlan executionPlan = planner.getPlan(ast, plannerContext);
            Assert.assertTrue(executionPlan.getPlan() instanceof LogicalReplace);
            canPushReplace((LogicalReplace) executionPlan.getPlan());
        } catch (NullPointerException t) {
            t.printStackTrace();
            throw t;
        }

        createTbDdl = "create table test_tb2(id int primary key, "
            + " a int, b int, c int, d int, data int, "
            + " clustered index gsi_t(c, d) dbpartition by hash (c, d), "
            + " local unique key uk(a, b, c, d)) dbpartition by hash(a, b)";
        buildTable(appName, createTbDdl);
        ast = parser.parse("replace into test_tb2 (a, b, c, d, data) values (10, 10, 10, 10, 10)", ec).get(0);
        try {
            Planner planner = new Planner();
            ExecutionPlan executionPlan = planner.getPlan(ast, plannerContext);
            Assert.assertTrue(executionPlan.getPlan() instanceof LogicalReplace);
            canNotPushReplace((LogicalReplace) executionPlan.getPlan());
        } catch (NullPointerException t) {
            t.printStackTrace();
            throw t;
        }
    }

    private void canPushReplace(LogicalReplace replace) {
        Assert.assertTrue(replace.isUkContainsAllSkAndGsiContainsAllUk());
        ReplaceRelocateWriter writer = replace.getPrimaryRelocateWriter();
        ec.setTxIsolation(Connection.TRANSACTION_READ_COMMITTED);
        Assert.assertTrue(writer.canPushReplace(ec));
        for (ReplaceRelocateWriter gsiRelocateWriter : replace.getGsiRelocateWriters()) {
            Assert.assertTrue(gsiRelocateWriter.canPushReplace(ec));
        }
    }

    private void canNotPushReplace(LogicalReplace replace) {
        Assert.assertFalse(replace.isUkContainsAllSkAndGsiContainsAllUk());
        ReplaceRelocateWriter writer = replace.getPrimaryRelocateWriter();
        ec.setTxIsolation(Connection.TRANSACTION_READ_COMMITTED);
        Assert.assertFalse(writer.canPushReplace(ec));
        for (ReplaceRelocateWriter gsiRelocateWriter : replace.getGsiRelocateWriters()) {
            Assert.assertFalse(gsiRelocateWriter.canPushReplace(ec));
        }
    }

    @Override
    protected String getPlan(String testSql) {
        return null;
    }
}
