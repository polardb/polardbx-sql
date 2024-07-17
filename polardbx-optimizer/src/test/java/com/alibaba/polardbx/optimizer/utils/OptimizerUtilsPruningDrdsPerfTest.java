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
public class OptimizerUtilsPruningDrdsPerfTest extends BasePlannerTest {
    protected RelOptCluster relOptCluster;
    protected RelOptSchema schema;

    public static final String SCHEMA_NAME = "optest";

    private static int testCount = 2;

    private static final String drdsTblInt = "CREATE TABLE multi_db_multi_tbl(\n"
        + " id bigint not null auto_increment, \n"
        + " bid int, \n"
        + " name varchar(30), \n"
        + " primary key(id)\n"
        + ") dbpartition by hash(id) tbpartition by hash(id) tbpartitions 10";

    private static final String drdsTblIntNotSimple = "CREATE TABLE multi_db_multi_tbl_NotSimple(\n"
        + " id bigint not null auto_increment, \n"
        + " bid int, \n"
        + " name varchar(30), \n"
        + " primary key(id)\n"
        + ") dbpartition by hash(id) tbpartition by hash(bid) tbpartitions 10";

    private static final String drdsTblVarchar = "CREATE TABLE multi_db_multi_tbl_varchar(\n"
        + " id varchar(10) not null, \n"
        + " bid int, \n"
        + " name varchar(30), \n"
        + " primary key(id)\n"
        + ") dbpartition by hash(id) tbpartition by hash(id) tbpartitions 10";

    private static final String drdsTblVarcharNotSimple = "CREATE TABLE multi_db_multi_tbl_varchar_NotSimple(\n"
        + " id varchar(10) not null, \n"
        + " bid int, \n"
        + " name varchar(30), \n"
        + " primary key(id)\n"
        + ") dbpartition by hash(id) tbpartition by hash(name) tbpartitions 10";

    public OptimizerUtilsPruningDrdsPerfTest() {
        super(SCHEMA_NAME);
    }

    @Before
    public void init() throws Exception {
        relOptCluster = SqlConverter.getInstance(SCHEMA_NAME, new ExecutionContext()).createRelOptCluster();
        schema = SqlConverter.getInstance(SCHEMA_NAME, new ExecutionContext()).getCatalog();
        loadStatistic();
        buildTable(SCHEMA_NAME, drdsTblInt);
        buildTable(SCHEMA_NAME, drdsTblIntNotSimple);
        buildTable(SCHEMA_NAME, drdsTblVarchar);
        buildTable(SCHEMA_NAME, drdsTblVarcharNotSimple);
    }

    @Override
    protected String getPlan(String testSql) {
        return null;
    }

    @Ignore
    @Override
    public void testSql() {
    }

    @Test
    public void testDrdsTblInt() {
        // bjaseline:6ms
        long pruneTime = testWithTblName("multi_db_multi_tbl", false);
    }

    @Ignore
    @Test
    public void testDrdsTblIntNotSimple() {
        // bjaseline:4544ms
        long pruneTime = testWithTblName("multi_db_multi_tbl_NotSimple", false);
    }

    @Test
    public void testDrdsTblVarchar() {
        // baseline:5ms
        long pruneTime = testWithTblName("multi_db_multi_tbl_varchar", true);
    }

    @Ignore
    @Test
    public void testDrdsTblVarcharNotSimple() {
        // baseline:3856ms
        long pruneTime = testWithTblName("multi_db_multi_tbl_varchar_NotSimple", true);
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
