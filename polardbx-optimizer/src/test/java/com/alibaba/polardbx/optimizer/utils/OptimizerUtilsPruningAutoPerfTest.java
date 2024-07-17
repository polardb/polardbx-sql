package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.jdbc.RawString;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.planner.common.BasePlannerTest;
import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author fangwu
 */
public class OptimizerUtilsPruningAutoPerfTest extends BasePlannerTest {
    protected RelOptCluster relOptCluster;
    protected RelOptSchema schema;

    public static final String SCHEMA_NAME = "optest";

    private static int testCount = 2;

    private static final String autoTbl = "CREATE TABLE key_tbl(\n"
        + " id bigint not null auto_increment,\n"
        + " bid int,\n"
        + " name varchar(30),\n"
        + " birthday datetime not null,\n"
        + " primary key(id)\n"
        + ")\n"
        + "PARTITION BY KEY(id)\n"
        + "PARTITIONS 64";

    private static final String autoTblHash = "CREATE TABLE hash_tbl(\n"
        + " id bigint not null auto_increment,\n"
        + " bid int,\n"
        + " name varchar(30),\n"
        + " birthday datetime not null,\n"
        + " primary key(id)\n"
        + ")\n"
        + "partition by hash(id)\n"
        + "partitions 64";

    private static final String autoTblRange = "CREATE TABLE orders(\n"
        + " order_id int,\n"
        + " order_time datetime not null)\n"
        + "PARTITION BY range columns(order_id)\n"
        + "(\n"
        + "  PARTITION p1 VALUES LESS THAN (1000),\n"
        + "  PARTITION p2 VALUES LESS THAN (2000),\n"
        + "  PARTITION p3 VALUES LESS THAN (3000),\n"
        + "  PARTITION p4 VALUES LESS THAN (4000),\n"
        + "  PARTITION p5 VALUES LESS THAN (5000),\n"
        + "  PARTITION p6 VALUES LESS THAN (MAXVALUE)\n"
        + ")";

    private static final String autoTblHashVarchar = "CREATE TABLE hash_varchar_tbl(\n"
        + " id varchar(10) not null,\n"
        + " bid int,\n"
        + " name varchar(30),\n"
        + " birthday datetime not null,\n"
        + " primary key(id)\n"
        + ")\n"
        + "partition by hash(id)\n"
        + "partitions 64";

    public OptimizerUtilsPruningAutoPerfTest() {
        super(SCHEMA_NAME);
    }

    @Before
    public void init() throws Exception {
        relOptCluster = SqlConverter.getInstance(SCHEMA_NAME, new ExecutionContext()).createRelOptCluster();
        schema = SqlConverter.getInstance(SCHEMA_NAME, new ExecutionContext()).getCatalog();
        loadStatistic();
        buildTable(SCHEMA_NAME, autoTbl);
        buildTable(SCHEMA_NAME, autoTblHash);
        buildTable(SCHEMA_NAME, autoTblRange);
        buildTable(SCHEMA_NAME, autoTblHashVarchar);
    }

    @Override
    protected String getPlan(String testSql) {
        return null;
    }

    @Override
    protected void initBasePlannerTestEnv() {
        this.useNewPartDb = true;
    }

    @Override
    @Ignore
    public void testSql() {
    }

    @Test
    public void testAutoTblKeyInt() {
        // baseline:405ms
        long pruneTime = testWithTblName("key_tbl", false);
    }

    @Test
    public void testAutoTblHashInt() {
        // baseline:422ms
        long pruneTime = testWithTblName("hash_tbl", false);
    }

    @Test
    public void testAutoTblHashVarchar() {
        // baseline:434ms
        long pruneTime = testWithTblName("hash_varchar_tbl", true);
    }

    @Test
    public void testAutoTblRangeInt() {
        //baseline 210ms
        long pruneTime = testWithTblName("orders", false);
    }

    private long testWithTblName(String tblName, boolean isVarchar) {
        ExecutionContext executionContext = new ExecutionContext(SCHEMA_NAME);
        Parameters parameters = new Parameters();
        Map<Integer, ParameterContext> params = Maps.newHashMap();
        if (isVarchar) {
            List<String> strList = Lists.newArrayList();
            for (int i = 0; i < 100000; i++) {
                strList.add(GeneralUtil.randomString(10));
            }
            params.put(1, new ParameterContext(ParameterMethod.setObject1, new Object[] {1, new RawString(strList)}));
        } else {
            params.put(1, new ParameterContext(ParameterMethod.setObject1, new Object[] {
                1, new RawString(IntStream.range(0, 100000).boxed().collect(
                Collectors.toList()))}));
        }

        parameters.setParams(params);
        executionContext.setParams(parameters);

        relOptCluster.getPlanner().getContext().unwrap(PlannerContext.class).setExecutionContext(executionContext);
        final RelDataTypeFactory typeFactory = relOptCluster.getTypeFactory();
        LogicalTableScan scan = LogicalTableScan.create(relOptCluster,
            schema.getTableForMember(Arrays.asList(SCHEMA_NAME, tblName)));
        LogicalView logicalView = LogicalView.create(scan, scan.getTable());
        RexBuilder rexBuilder = relOptCluster.getRexBuilder();
        RexNode row =
            rexBuilder.makeCall(SqlStdOperatorTable.ROW, rexBuilder.makeDynamicParam(typeFactory.createSqlType(
                SqlTypeName.INTEGER), 0));
        RexNode in = rexBuilder.makeCall(SqlStdOperatorTable.IN, rexBuilder.makeInputRef(logicalView, 0), row);

        LogicalFilter logicalFilter = LogicalFilter.create(logicalView, in);
        logicalView.push(logicalFilter);
        for (int i = 0; i < testCount; i++) {
            OptimizerUtils.pruningInValue(logicalView, executionContext);
        }
        long avg = executionContext.getPruningTime() / testCount;
        System.out.println("avg pruning time(ms):" + avg);
        return avg;
    }
}
