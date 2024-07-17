package com.alibaba.polardbx.planner.flashback;

import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.planner.common.BasePlannerTest;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.TDDLSqlSelect;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.AS_OF_57;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.AS_OF_80;

/**
 * @author lijiu.lzw
 */
public class FlashbackQueryTsoTest extends BasePlannerTest {
    private static boolean isMySQL80;

    public FlashbackQueryTsoTest() {
        super("FlashbackQueryTsoTest");
    }

    @BeforeClass
    public static void before() {
        isMySQL80 = InstanceVersion.isMYSQL80();
    }

    @AfterClass
    public static void after() {
        InstanceVersion.setMYSQL80(isMySQL80);
    }

    @Override
    protected String getPlan(String testSql) {
        return null;
    }

    protected ExecutionPlan getExecutionPlan(ExecutionContext executionContext, String testSql) {
        SqlNode ast = new FastsqlParser().parse(testSql, executionContext).get(0);
        PlannerContext plannerContext = PlannerContext.fromExecutionContext(executionContext);

        return Planner.getInstance().getPlan(ast, plannerContext);
    }

    protected void prepareTable() throws Exception {
        String tableSql = "CREATE TABLE `asOfTsoTest` (\n"
            + "    `id` bigint NOT NULL AUTO_INCREMENT,\n"
            + "    `k` int(11) NOT NULL DEFAULT '0',\n"
            + "    `c` char(120) NOT NULL DEFAULT '',\n"
            + "    `pad` char(60) NOT NULL DEFAULT '',\n"
            + "    PRIMARY KEY (`id`),\n"
            + "    INDEX `k_1` (`k`)\n"
            + ") dbpartition by hash(id);";
        buildTable(getAppName(), tableSql);
    }

    @Test
    public void test57Tso() throws Exception {
        InstanceVersion.setMYSQL80(false);

        prepareTable();

        String sql = "select * from asOfTsoTest as of tso 10 + tso_timestamp()";
        ExecutionContext executionContext = new ExecutionContext(appName);
        ExecutionPlan plan = getExecutionPlan(executionContext, sql);

        String planStr = RelUtils.toString(plan.getPlan(), null,
            RexUtils.getEvalFunc(executionContext), executionContext);

        String expected = "Gather(concurrent=true)\n"
            + "  LogicalView(tables=\"[0000-0003].asOfTsoTest\", shardCount=4, sql=\"SELECT `id`, `k`, `c`, `pad` FROM `asOfTsoTest` AS OF TSO (10 + TSO_TIMESTAMP()) AS `asOfTsoTest`\")\n";
        Assert.assertEquals(expected, planStr);
        TDDLSqlSelect select = (TDDLSqlSelect) plan.getAst();
        SqlCall from = (SqlCall) select.getFrom();
        Assert.assertEquals(SqlKind.AS_OF, from.getKind());
        Assert.assertEquals(AS_OF_57, from.getOperator());

    }

    @Test
    public void test80Tso() throws Exception {
        InstanceVersion.setMYSQL80(true);

        prepareTable();

        String sql = "select * from asOfTsoTest as of tso 1000";
        ExecutionContext executionContext = new ExecutionContext(appName);
        ExecutionPlan plan = getExecutionPlan(executionContext, sql);

        String planStr = RelUtils.toString(plan.getPlan(), null,
            RexUtils.getEvalFunc(executionContext), executionContext);

        String expected = "Gather(concurrent=true)\n"
            + "  LogicalView(tables=\"[0000-0003].asOfTsoTest\", shardCount=4, sql=\"SELECT `id`, `k`, `c`, `pad` FROM `asOfTsoTest` AS OF GCN 1000 AS `asOfTsoTest`\")\n";
        Assert.assertEquals(expected, planStr);
        TDDLSqlSelect select = (TDDLSqlSelect) plan.getAst();
        SqlCall from = (SqlCall) select.getFrom();
        Assert.assertEquals(SqlKind.AS_OF, from.getKind());
        Assert.assertEquals(AS_OF_80, from.getOperator());
    }

    @Test
    public void test80TsoExpress() throws Exception {
        InstanceVersion.setMYSQL80(true);

        prepareTable();

        String sql = "select * from asOfTsoTest as of tso 1000 + 2000 * tso_timestamp()";
        ExecutionContext executionContext = new ExecutionContext(appName);
        ExecutionPlan plan = getExecutionPlan(executionContext, sql);

        String planStr = RelUtils.toString(plan.getPlan(), null,
            RexUtils.getEvalFunc(executionContext), executionContext);

        String expected = "Gather(concurrent=true)\n"
            + "  LogicalView(tables=\"[0000-0003].asOfTsoTest\", shardCount=4, sql=\"SELECT `id`, `k`, `c`, `pad` FROM `asOfTsoTest` AS OF GCN (1000 + (2000 * TSO_TIMESTAMP())) AS `asOfTsoTest`\")\n";
        Assert.assertEquals(expected, planStr);
        TDDLSqlSelect select = (TDDLSqlSelect) plan.getAst();
        SqlCall from = (SqlCall) select.getFrom();
        Assert.assertEquals(SqlKind.AS_OF, from.getKind());
        Assert.assertEquals(AS_OF_80, from.getOperator());
    }

}
