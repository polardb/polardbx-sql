package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.alibaba.polardbx.optimizer.parse.visitor.ContextParameters;
import com.alibaba.polardbx.planner.common.BasePlannerTest;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.SQLSyntaxErrorException;
import java.util.Arrays;

public class BuildFinalPlanVisitorTest extends BasePlannerTest {

    static String SCHEMA_NAME = "FINAL_PLAN_TEST";

    String empDDL = "CREATE TABLE emp(\n" + "  userId int, \n" + "  name varchar(30), \n" + "  operation tinyint(1), \n"
        + "  actionDate varchar(30)\n, primary key(userId)" + "  ) ";

    public BuildFinalPlanVisitorTest() {
        super(SCHEMA_NAME, true);
    }

    @Test
    public void testSingleTableScan() throws SQLSyntaxErrorException {
        initAppNameConfig(SCHEMA_NAME);
        buildTable(SCHEMA_NAME, empDDL);

        String sql = "select * from emp where userid=1";
        ExecutionContext ec = new ExecutionContext();

        OptimizerContext oc = getContextByAppName(SCHEMA_NAME);
        SchemaManager schemaManager = oc.getLatestSchemaManager();

        ec.setSchemaManager(SCHEMA_NAME, schemaManager);
        PlannerContext pc = PlannerContext.fromExecutionContext(ec);

        ContextParameters contextParameters = new ContextParameters(false);
        SqlNodeList astList = new FastsqlParser().parse(ByteString.from(sql), null, contextParameters, ec);
        // parameterizedSql can not be a multiStatement.
        SqlNode ast = astList.get(0);

        BuildFinalPlanVisitor visitor = new BuildFinalPlanVisitor(ast, pc);

        RelOptCluster relOptCluster = SqlConverter.getInstance(appName, new ExecutionContext()).createRelOptCluster();
        RelOptSchema schema = SqlConverter.getInstance(appName, new ExecutionContext()).getCatalog();

        LogicalTableScan scan = LogicalTableScan.create(relOptCluster,
            schema.getTableForMember(Arrays.asList(SCHEMA_NAME, "emp")));
        LogicalFilter filter =
            LogicalFilter.create(scan, relOptCluster.getRexBuilder().makeCall(SqlStdOperatorTable.EQUALS,
                relOptCluster.getRexBuilder()
                    .makeFieldAccess(relOptCluster.getRexBuilder().makeRangeReference(scan), "userId", true),
                relOptCluster.getRexBuilder().makeExactLiteral(BigDecimal.TEN)));
        LogicalView logicalView = LogicalView.create(filter, scan.getTable());

        RelNode rel = logicalView.accept(visitor);
        System.out.println(rel);
        assert rel == logicalView;
    }

    @Override
    protected String getPlan(String testSql) {
        return "";
    }
}
