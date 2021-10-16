/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.planner.sharding;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.sharding.ConditionExtractor;
import com.alibaba.polardbx.optimizer.sharding.label.Label;
import com.alibaba.polardbx.optimizer.sharding.result.ConditionResult;
import com.alibaba.polardbx.optimizer.sharding.result.ExtractionResult;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.planner.common.PlanTestCommon;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.BitSets;
import org.apache.calcite.util.Util;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.BitSet;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author chenmo.cm
 */
@Ignore
public class ConditionExtractorTest extends PlanTestCommon {

    public ConditionExtractorTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return ImmutableList.of(new Object[] {"ConditionExtractorTest", 0, "", "", "0"});
    }

    @Override
    public void testSql() {

    }

    /**
     * Converts a relational expression to a string with linux line-endings.
     */
    private String str(RelNode r) {
        return Util.toLinux(RelOptUtil.toString(r));
    }

    private String str(RelDataType r) {
        return Util.toLinux(r.toString());
    }

    private String str(RexNode r) {
        return Util.toLinux(r.toString());
    }

    private void checkRowType(RelNode root, String rowType, String fullRowType) {
        checkRowType(root, rowType, fullRowType, false);
    }

    private void checkRowType(RelNode root, String rowType, String fullRowType, boolean fullTypeString) {
        final ExtractionResult extractionResult = ConditionExtractor.partitioningConditionFrom(root).extract();

        //final RelDataType relDataType = visitor.context.getRoot().buildRowType();

        final Label rootLabel = extractionResult.getRootLabel();
        System.out.println(rootLabel.toString());

        if (fullTypeString) {
            assertThat(Util.toLinux(rootLabel.getRowType().getFullTypeString()), is(rowType));
            assertThat(Util.toLinux(rootLabel.getFullRowType().fullRowType.getFullTypeString()), is(fullRowType));
        } else {
            assertThat(str(rootLabel.getRowType()), is(rowType));
            assertThat(str(rootLabel.getFullRowType().fullRowType), is(fullRowType));

        }
    }

    private void checkCondition(RelNode root, ConditionResult condition, String s) {
        final List<RexNode> rexNodes = condition.toRexNodes();

        final RexBuilder rexBuilder = root.getCluster().getRexBuilder();
        final RexNode conditions = RexUtil.composeConjunction(rexBuilder, rexNodes, false);
        final String conditionStr = str(conditions);
        assertThat(conditionStr, is(s));

        System.out.println(conditionStr);
    }

    private RelBuilder getRelBuilder() {
        return RelFactories.LOGICAL_BUILDER.create(
            SqlConverter.getInstance(appName, new ExecutionContext()).createRelOptCluster(PlannerContext.EMPTY_CONTEXT),
            RelUtils.buildCatalogReader(OptimizerContext.getContext(appName).getSchemaName(), new ExecutionContext()));
    }

    @Test
    public void testScan() {
        // Equivalent SQL:
        // SELECT *
        // FROM emp
        final RelNode root = getRelBuilder().scan("EMP").build();
        assertThat(str(root), is("LogicalTableScan(table=[[" + appName + ", EMP]])\n"));
    }

    @Test
    public void testProject() {
        // Equivalent SQL:
        // SELECT deptno, CAST(comm AS SMALLINT) AS comm, 20 AS $f2,
        // comm AS comm3, comm AS c
        // FROM emp
        final RelBuilder builder = getRelBuilder();
        RelNode root = builder.scan("EMP")
            .project(builder.field("DEPTNO"),
                builder.cast(builder.field(6), SqlTypeName.SMALLINT),
                builder.literal(20),
                builder.field(6),
                builder.alias(builder.field(6), "C"))
            .build();
        // Note: CAST(COMM) gets the COMM alias because it occurs first
        // Note: AS(COMM, C) becomes just $6
        assertThat(str(root),
            is("LogicalProject(DEPTNO=[$7], COMM=[$6], $f2=[20], COMM0=[$6], C=[$6])\n"
                + "  LogicalTableScan(table=[[" + appName + ", EMP]])\n"));
    }

    @Test
    public void testRowTypeTableScan() {
        // Equivalent SQL:
        // SELECT *
        // FROM emp
        final RelNode root = getRelBuilder().scan("EMP").build();
        assertThat(str(root), is("LogicalTableScan(table=[[" + appName + ", EMP]])\n"));

        checkRowType(root,
            "RecordType(INTEGER EMPNO, VARCHAR(10) ENAME, VARCHAR(9) JOB, SMALLINT MGR, DATE HIREDATE, DECIMAL(7, 2) SAL, DECIMAL(7, 2) COMM, INTEGER DEPTNO)",
            "RecordType(INTEGER EMPNO, VARCHAR(10) ENAME, VARCHAR(9) JOB, SMALLINT MGR, DATE HIREDATE, DECIMAL(7, 2) SAL, DECIMAL(7, 2) COMM, INTEGER DEPTNO)");
    }

    @Test
    public void testRowTypeProject1() {
        // Equivalent SQL:
        // SELECT deptno, CAST(comm AS SMALLINT) AS comm, 20 AS $f2,
        // comm AS comm3, comm AS c
        // FROM emp
        final RelBuilder builder = getRelBuilder();
        RelNode root = builder.scan("EMP")
            .project(builder.field("DEPTNO"),
                builder.cast(builder.field(6), SqlTypeName.SMALLINT),
                builder.literal(20),
                builder.field(6),
                builder.alias(builder.field(6), "C"))
            .build();
        // Note: CAST(COMM) gets the COMM alias because it occurs first
        // Note: AS(COMM, C) becomes just $6
        assertThat(str(root),
            is("LogicalProject(DEPTNO=[$7], COMM=[$6], $f2=[20], COMM0=[$6], C=[$6])\n"
                + "  LogicalTableScan(table=[[" + appName + ", EMP]])\n"));

        checkRowType(root,
            "RecordType(INTEGER DEPTNO, DECIMAL(7, 2) COMM, BIGINT $f2, DECIMAL(7, 2) COMM, DECIMAL(7, 2) COMM)",
            "RecordType(INTEGER EMPNO, VARCHAR(10) ENAME, VARCHAR(9) JOB, SMALLINT MGR, DATE HIREDATE, DECIMAL(7, 2) SAL, DECIMAL(7, 2) COMM, INTEGER DEPTNO)");
    }

    @Test
    public void testRowTypeProject2() {
        final RelBuilder builder = getRelBuilder();
        RelNode root = builder.scan("EMP")
            .project(builder.field("DEPTNO"),
                builder.cast(builder.field(6), SqlTypeName.SMALLINT),
                builder.or(builder.equals(builder.field("DEPTNO"), builder.literal(20)),
                    builder.and(builder.literal(null), builder.equals(builder.field("DEPTNO"), builder.literal(10)),
                        builder.and(builder.isNull(builder.field(6)),
                            builder.not(builder.isNotNull(builder.field(7))))),
                    builder.equals(builder.field("DEPTNO"), builder.literal(20)),
                    builder.equals(builder.field("DEPTNO"), builder.literal(30))),
                builder.alias(builder.isNull(builder.field(2)), "n2"),
                builder.alias(builder.isNotNull(builder.field(3)), "nn2"),
                builder.literal(20),
                builder.field(6),
                builder.alias(builder.field(6), "C"))
            .build();
        assertThat(str(root),
            is("LogicalProject(DEPTNO=[$7], COMM=[$6], $f2=[OR(=($7, 20), AND(null, =($7, 10), IS NULL($6), IS NULL($7)), =($7, 30))], n2=[IS NULL($2)], nn2=[IS NOT NULL($3)], $f5=[20], COMM0=[$6], C=[$6])\n"
                + "  LogicalTableScan(table=[[" + appName + ", EMP]])\n"));

        checkRowType(root,
            "RecordType(INTEGER DEPTNO, DECIMAL(7, 2) COMM, BIGINT $f2, BIGINT n2, BIGINT nn2, BIGINT $f5, DECIMAL(7, 2) COMM, DECIMAL(7, 2) COMM)",
            "RecordType(INTEGER EMPNO, VARCHAR(10) ENAME, VARCHAR(9) JOB, SMALLINT MGR, DATE HIREDATE, DECIMAL(7, 2) SAL, DECIMAL(7, 2) COMM, INTEGER DEPTNO)");
    }

    @Test
    public void testRowTypeProject3() {
        final RelBuilder builder = getRelBuilder();
        RelNode root = builder.scan("EMP")
            .project(builder.field("DEPTNO"),
                builder.cast(builder.field(6), SqlTypeName.SMALLINT),
                builder.or(builder.equals(builder.field("DEPTNO"), builder.literal(20)),
                    builder.and(builder.literal(null),
                        builder.equals(builder.field("DEPTNO"), builder.literal(10)),
                        builder.and(builder.isNull(builder.field(6)),
                            builder.not(builder.isNotNull(builder.field(7))))),
                    builder.equals(builder.field("DEPTNO"), builder.literal(20)),
                    builder.equals(builder.field("DEPTNO"), builder.literal(30))),
                builder.alias(builder.isNull(builder.field(2)), "n2"),
                builder.alias(builder.isNotNull(builder.field(3)), "nn2"),
                builder.literal(20),
                builder.field(6),
                builder.alias(builder.field(6), "C"))
            .project(builder.field("C"), builder.field(0), builder.and(builder.field(2), builder.field(4)),
                builder.field("COMM"))
            .build();
        assertThat(str(root),
            is("LogicalProject(C=[$7], DEPTNO=[$0], $f2=[AND($2, $4)], COMM=[$1])\n"
                + "  LogicalProject(DEPTNO=[$7], COMM=[$6], $f2=[OR(=($7, 20), AND(null, =($7, 10), IS NULL($6), IS "
                + "NULL($7)), =($7, 30))], n2=[IS NULL($2)], nn2=[IS NOT NULL($3)], $f5=[20], COMM0=[$6], C=[$6])\n"
                + "    LogicalTableScan(table=[[" + appName + ", EMP]])\n"));

        checkRowType(root,
            "RecordType(DECIMAL(7, 2) COMM, INTEGER DEPTNO, BIGINT $f2, DECIMAL(7, 2) COMM)",
            "RecordType(INTEGER EMPNO, VARCHAR(10) ENAME, VARCHAR(9) JOB, SMALLINT MGR, DATE HIREDATE, DECIMAL(7, 2) SAL, DECIMAL(7, 2) COMM, INTEGER DEPTNO)");
    }

    @Test
    public void testRowTypeProjectIdentityWithFieldsRename() {
        final RelBuilder builder = getRelBuilder();
        RelNode root = builder.scan("DEPT")
            .project(builder.alias(builder.field(0), "a"),
                builder.alias(builder.field(1), "b"),
                builder.alias(builder.field(2), "c"))
            .as("t1")
            .project(builder.field("a"), builder.field("t1", "c"))
            .build();
        final String expected =
            "LogicalProject(a=[$0], c=[$2])\n" + "  LogicalTableScan(table=[[" + appName + ", DEPT]])\n";
        assertThat(str(root), is(expected));

        checkRowType(root,
            "RecordType(INTEGER DEPTNO, VARCHAR(13) LOC)",
            "RecordType(INTEGER DEPTNO, VARCHAR(14) DNAME, VARCHAR(13) LOC)");
    }

    @Test
    public void testRowTypeJoin1() {
        // Equivalent SQL:
        // SELECT *
        // FROM (SELECT * FROM emp WHERE comm IS NULL)
        // JOIN dept ON emp.deptno = dept.deptno
        final RelBuilder builder = getRelBuilder();
        RelNode root = builder.scan("EMP")
            .filter(builder.call(SqlStdOperatorTable.IS_NULL, builder.field("COMM")))
            .scan("DEPT")
            .join(JoinRelType.INNER,
                builder.call(SqlStdOperatorTable.EQUALS, builder.field(2, 0, "DEPTNO"), builder.field(2, 1, "DEPTNO")))
            .build();
        final String expected = "" + "LogicalJoin(condition=[=($7, $8)], joinType=[INNER])\n"
            + "  LogicalFilter(condition=[IS NULL($6)])\n"
            + "    LogicalTableScan(table=[[" + appName + ", EMP]])\n"
            + "  LogicalTableScan(table=[[" + appName + ", DEPT]])\n";
        assertThat(str(root), is(expected));

        checkRowType(root,
            "RecordType(INTEGER EMPNO, VARCHAR(10) ENAME, VARCHAR(9) JOB, SMALLINT MGR, DATE HIREDATE, DECIMAL(7, 2) SAL, DECIMAL(7, 2) COMM, INTEGER DEPTNO, INTEGER DEPTNO, VARCHAR(14) DNAME, VARCHAR(13) LOC)",
            "RecordType(INTEGER EMPNO, VARCHAR(10) ENAME, VARCHAR(9) JOB, SMALLINT MGR, DATE HIREDATE, DECIMAL(7, 2) SAL, DECIMAL(7, 2) COMM, INTEGER DEPTNO, INTEGER DEPTNO, VARCHAR(14) DNAME, VARCHAR(13) LOC)");
    }

    @Test
    public void testRowTypeJoin2() {
        // Equivalent SQL:
        // SELECT *
        // FROM (SELECT DEPTNO,
        //         CAST(COMM AS SMALLINT),
        //         (DEPTNO = 20 OR (null AND DEPTNO = 10 AND (COMM IS NULL AND NOT DEPTNO IS NOT NULL)) OR DEPTNO = 20 OR DEPTNO = 30),
        //         JOB IS NULL AS n2,
        //         MGR IS NOT NULL AS nn2,
        //         20,
        //         COMM,
        //         COMM AS C
        //       FROM emp WHERE comm IS NULL)
        // JOIN dept ON emp.deptno = dept.deptno
        final RelBuilder builder = getRelBuilder();
        RelNode root = builder.scan("EMP")
            .filter(builder.call(SqlStdOperatorTable.IS_NULL, builder.field("COMM")))
            .project(builder.field("DEPTNO"),
                builder.cast(builder.field(6), SqlTypeName.SMALLINT),
                builder.or(builder.equals(builder.field("DEPTNO"), builder.literal(20)),
                    builder.and(builder.literal(null),
                        builder.equals(builder.field("DEPTNO"), builder.literal(10)),
                        builder.and(builder.isNull(builder.field(6)),
                            builder.not(builder.isNotNull(builder.field(7))))),
                    builder.equals(builder.field("DEPTNO"), builder.literal(20)),
                    builder.equals(builder.field("DEPTNO"), builder.literal(30))),
                builder.alias(builder.isNull(builder.field(2)), "n2"),
                builder.alias(builder.isNotNull(builder.field(3)), "nn2"),
                builder.literal(20),
                builder.field(6),
                builder.alias(builder.field(6), "C"))
            .scan("DEPT")
            .join(JoinRelType.INNER,
                builder.call(SqlStdOperatorTable.EQUALS, builder.field(2, 0, "DEPTNO"), builder.field(2, 1, "DEPTNO")))
            .build();
        final String expected = "LogicalJoin(condition=[=($0, $8)], joinType=[INNER])\n"
            + "  LogicalProject(DEPTNO=[$7], COMM=[$6], $f2=[OR(=($7, 20), AND(null, =($7, 10), IS NULL($6), IS NULL($7)), =($7, 30))], n2=[IS NULL($2)], nn2=[IS NOT NULL($3)], $f5=[20], COMM0=[$6], C=[$6])\n"
            + "    LogicalFilter(condition=[IS NULL($6)])\n"
            + "      LogicalTableScan(table=[[" + appName + ", EMP]])\n"
            + "  LogicalTableScan(table=[[" + appName + ", DEPT]])\n";
        assertThat(str(root), is(expected));

        checkRowType(root,
            "RecordType(INTEGER DEPTNO, DECIMAL(7, 2) COMM, BIGINT $f2, BIGINT n2, BIGINT nn2, BIGINT $f5, DECIMAL(7, 2) COMM, DECIMAL(7, 2) COMM, INTEGER DEPTNO, VARCHAR(14) DNAME, VARCHAR(13) LOC)",
            "RecordType(INTEGER EMPNO, VARCHAR(10) ENAME, VARCHAR(9) JOB, SMALLINT MGR, DATE HIREDATE, DECIMAL(7, 2) SAL, DECIMAL(7, 2) COMM, INTEGER DEPTNO, INTEGER DEPTNO, VARCHAR(14) DNAME, VARCHAR(13) LOC)");
    }

    @Test
    public void testRowTypeJoin3() {
        // Equivalent SQL:
        // SELECT *
        // FROM bonus JOIN (SELECT DEPTNO AS C, ENAME FROM emp WHERE comm IS NULL) b ON bonus.ename = b.ename
        // JOIN dept ON b.C = dept.deptno
        final RelBuilder builder = getRelBuilder();
        RelNode root = builder.scan("BONUS")
            .scan("EMP")
            .filter(builder.call(SqlStdOperatorTable.IS_NULL, builder.field("COMM")))
            .project(builder.alias(builder.field("DEPTNO"), "C"), builder.field("ENAME"))
            .join(JoinRelType.INNER,
                builder.call(SqlStdOperatorTable.EQUALS, builder.field(2, 0, "ENAME"), builder.field(2, 1, "ENAME")))
            .scan("DEPT")
            .join(JoinRelType.INNER,
                builder.call(SqlStdOperatorTable.EQUALS, builder.field(2, 0, "C"), builder.field(2, 1, "DEPTNO")))
            .build();
        final String expected = "LogicalJoin(condition=[=($4, $6)], joinType=[INNER])\n"
            + "  LogicalJoin(condition=[=($0, $5)], joinType=[INNER])\n"
            + "    LogicalTableScan(table=[[" + appName + ", BONUS]])\n"
            + "    LogicalProject(C=[$7], ENAME=[$1])\n"
            + "      LogicalFilter(condition=[IS NULL($6)])\n"
            + "        LogicalTableScan(table=[[" + appName + ", EMP]])\n"
            + "  LogicalTableScan(table=[[" + appName + ", DEPT]])\n";
        assertThat(str(root), is(expected));

        checkRowType(root,
            "RecordType(VARCHAR(10) ENAME, VARCHAR(9) JOB, DECIMAL(7, 2) SAL, DECIMAL(7, 2) COMM, INTEGER DEPTNO, VARCHAR(10) ENAME, INTEGER DEPTNO, VARCHAR(14) DNAME, VARCHAR(13) LOC)",
            "RecordType(VARCHAR(10) ENAME, VARCHAR(9) JOB, DECIMAL(7, 2) SAL, DECIMAL(7, 2) COMM, INTEGER EMPNO, VARCHAR(10) ENAME, VARCHAR(9) JOB, SMALLINT MGR, DATE HIREDATE, DECIMAL(7, 2) SAL, DECIMAL(7, 2) COMM, INTEGER DEPTNO, INTEGER DEPTNO, VARCHAR(14) DNAME, VARCHAR(13) LOC)");
    }

    @Test
    public void testRowTypeAggregate1() {
        // Equivalent SQL:
        //   SELECT COUNT(*) AS c, SUM(mgr + 1) AS s
        //   FROM emp
        //   GROUP BY ename, hiredate + mgr
        final RelBuilder builder = getRelBuilder();
        RelNode root =
            builder.scan("EMP")
                .aggregate(
                    builder.groupKey(builder.field(1),
                        builder.call(SqlStdOperatorTable.PLUS,
                            builder.field(4),
                            builder.field(3)),
                        builder.field(1)),
                    builder.aggregateCall(SqlStdOperatorTable.COUNT, false, false,
                        null, "C"),
                    builder.aggregateCall(SqlStdOperatorTable.SUM, false, false,
                        null, "S",
                        builder.call(SqlStdOperatorTable.PLUS, builder.field(3),
                            builder.literal(1))))
                .build();
        assertThat(str(root),
            is(""
                + "LogicalAggregate(group=[{1, 8}], C=[COUNT()], S=[SUM($9)])\n"
                + "  LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7], $f8=[+($4, $3)], $f9=[+($3, 1)])\n"
                + "    LogicalTableScan(table=[[" + appName + ", EMP]])\n"));

        checkRowType(root,
            "RecordType(VARCHAR(10) ENAME, BIGINT $f8, BIGINT C, BIGINT S)",
            "RecordType(INTEGER EMPNO, VARCHAR(10) ENAME, VARCHAR(9) JOB, SMALLINT MGR, DATE HIREDATE, DECIMAL(7, 2) SAL, DECIMAL(7, 2) COMM, INTEGER DEPTNO)");
    }

    @Test
    public void testRowTypeAggregateProjectWithAliases() {
        final RelBuilder builder = getRelBuilder();
        RelNode root = builder.scan("EMP")
            .project(builder.field("DEPTNO"))
            .aggregate(builder.groupKey(builder.alias(builder.field("DEPTNO"), "departmentNo")))
            .build();
        final String expected = "" + "LogicalAggregate(group=[{0}])\n" + "  LogicalProject(departmentNo=[$0])\n"
            + "    LogicalProject(DEPTNO=[$7])\n"
            + "      LogicalTableScan(table=[[" + appName + ", EMP]])\n";
        assertThat(str(root), is(expected));

        checkRowType(root,
            "RecordType(INTEGER DEPTNO)",
            "RecordType(INTEGER EMPNO, VARCHAR(10) ENAME, VARCHAR(9) JOB, SMALLINT MGR, DATE HIREDATE, DECIMAL(7, 2) SAL, DECIMAL(7, 2) COMM, INTEGER DEPTNO)");
    }

    @Test
    public void testRowTypeAggregateProjectWithExpression() {
        final RelBuilder builder = getRelBuilder();
        RelNode root = builder.scan("EMP")
            .project(builder.field("DEPTNO"))
            .aggregate(builder.groupKey(builder
                .alias(builder.call(SqlStdOperatorTable.PLUS, builder.field("DEPTNO"), builder.literal(3)), "d3")))
            .build();
        final String expected = "" + "LogicalAggregate(group=[{1}])\n" + "  LogicalProject(DEPTNO=[$0], d3=[$1])\n"
            + "    LogicalProject(DEPTNO=[$0], $f1=[+($0, 3)])\n"
            + "      LogicalProject(DEPTNO=[$7])\n"
            + "        LogicalTableScan(table=[[" + appName + ", EMP]])\n";
        assertThat(str(root), is(expected));

        checkRowType(root,
            "RecordType(BIGINT d3)",
            "RecordType(INTEGER EMPNO, VARCHAR(10) ENAME, VARCHAR(9) JOB, SMALLINT MGR, DATE HIREDATE, DECIMAL(7, 2) SAL, DECIMAL(7, 2) COMM, INTEGER DEPTNO)");
    }

    @Test
    public void testRowTypeValues() {
        // Equivalent SQL:
        // VALUES (true, 1), (false, -50) AS t(a, b)
        final RelBuilder builder = getRelBuilder();
        RelNode root = builder.values(new String[] {"a", "b"}, true, 1, false, -50).build();
        final String expected = "LogicalValues(tuples=[[{ true, 1 }, { false, -50 }]])\n";
        assertThat(str(root), is(expected));
        final String expectedType = "RecordType(BOOLEAN NOT NULL a, BIGINT NOT NULL b) NOT NULL";
        assertThat(root.getRowType().getFullTypeString(), is(expectedType));

        checkRowType(root, expectedType, expectedType, true);
    }

    @Test
    public void testRowTypeValues1() {
        // Equivalent SQL:
        //   VALUES (null, 1, 'abc'), (false, null, 'longer string')
        final RelBuilder builder = getRelBuilder();
        RelNode root =
            builder.values(new String[] {"a", null, "c"},
                null, 1, "abc",
                false, null, "longer string").build();
        final String expected =
            "LogicalValues(tuples=[[{ null, 1, _UTF-16'abc' }, { false, null, _UTF-16'longer string' }]])\n";
        assertThat(str(root), is(expected));
        final String expectedType =
            "RecordType(BOOLEAN a, BIGINT expr$1, CHAR(13) CHARACTER SET \"UTF-16\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL c) NOT NULL";
        assertThat(root.getRowType().getFullTypeString(), is(expectedType));

        checkRowType(root, expectedType, expectedType, true);
    }

    /**
     * Tests creating Values with some field names and some values null.
     */
    @Test
    public void testRowTypeValuesNullable() {
        // Equivalent SQL:
        // VALUES (null, 1, 'abc'), (false, null, 'longer string')
        final RelBuilder builder = getRelBuilder();
        RelNode root = builder.values(new String[] {"a", null, "c"}, null, 1, "abc", false, null, "longer string")
            .build();
        final String expected =
            "LogicalValues(tuples=[[{ null, 1, _UTF-16'abc' }, { false, null, _UTF-16'longer string' }]])\n";
        assertThat(str(root), is(expected));
        final String expectedType =
            "RecordType(BOOLEAN a, BIGINT expr$1, CHAR(13) CHARACTER SET \"UTF-16\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL c) NOT NULL";
        assertThat(root.getRowType().getFullTypeString(), is(expectedType));

        checkRowType(root, expectedType, expectedType, true);
    }

    @Test
    public void testRowTypeEmpty() {
        // Equivalent SQL:
        // SELECT deptno, true FROM dept LIMIT 0
        // optimized to
        // VALUES
        final RelBuilder builder = getRelBuilder();
        RelNode root = builder.scan("DEPT").project(builder.field(0), builder.literal(false)).empty().build();
        final String expected = "LogicalValues(tuples=[[]])\n";
        assertThat(str(root), is(expected));
        final String expectedType = "RecordType(INTEGER NOT NULL DEPTNO, BOOLEAN NOT NULL $f1) NOT NULL";
        assertThat(root.getRowType().getFullTypeString(), is(expectedType));
        checkRowType(root, expectedType, expectedType, true);
    }

    @Test
    public void testRowTypeUnion1() {
        // Equivalent SQL:
        // SELECT deptno FROM emp
        // UNION ALL
        // SELECT deptno FROM dept
        final RelBuilder builder = getRelBuilder();
        RelNode root = builder.scan("DEPT")
            .project(builder.field("DEPTNO"))
            .scan("EMP")
            .filter(builder.call(SqlStdOperatorTable.EQUALS, builder.field("DEPTNO"), builder.literal(20)))
            .project(builder.field("EMPNO"))
            .union(true)
            .build();
        assertThat(str(root),
            is("LogicalUnion(all=[true])\n"
                + "  LogicalProject(DEPTNO=[$0])\n"
                + "    LogicalTableScan(table=[[" + appName + ", DEPT]])\n"
                + "  LogicalProject(EMPNO=[$0])\n"
                + "    LogicalFilter(condition=[=($7, 20)])\n"
                + "      LogicalTableScan(table=[[" + appName + ", EMP]])\n"));

        checkRowType(root, "RecordType(INTEGER DEPTNO)", "RecordType(INTEGER DEPTNO)");
    }

    @Test
    public void testRowTypeUnion2() {
        // Equivalent SQL:
        // SELECT deptno FROM dept
        // UNION ALL
        // SELECT empno FROM emp
        // UNION ALL
        // SELECT deptno FROM emp
        final RelBuilder builder = getRelBuilder();
        RelNode root = builder.scan("DEPT")
            .project(builder.field("DEPTNO"))
            .scan("EMP")
            .project(builder.field("EMPNO"))
            .scan("EMP")
            .project(builder.field("DEPTNO"))
            .union(true, 3)
            .build();
        assertThat(str(root),
            is("LogicalUnion(all=[true])\n" + "  LogicalProject(DEPTNO=[$0])\n"
                + "    LogicalTableScan(table=[[" + appName + ", DEPT]])\n" + "  LogicalProject(EMPNO=[$0])\n"
                + "    LogicalTableScan(table=[[" + appName + ", EMP]])\n" + "  LogicalProject(DEPTNO=[$7])\n"
                + "    LogicalTableScan(table=[[" + appName + ", EMP]])\n"));

        checkRowType(root, "RecordType(INTEGER DEPTNO)", "RecordType(INTEGER DEPTNO)");
    }

    @Test
    public void testRowTypeUnionAlias() {
        final RelBuilder builder = getRelBuilder();
        RelNode root = builder.scan("EMP")
            .as("e1")
            .project(builder.field("EMPNO"),
                builder.call(SqlStdOperatorTable.CONCAT, builder.field("ENAME"), builder.literal("-1")))
            .scan("EMP")
            .as("e2")
            .project(builder.field("EMPNO"),
                builder.call(SqlStdOperatorTable.CONCAT, builder.field("ENAME"), builder.literal("-2")))
            .union(false) // aliases lost here
            .project(builder.fields(Lists.newArrayList(1, 0)))
            .build();
        final String expected = "" + "LogicalProject($f1=[$1], EMPNO=[$0])\n"
            + "  LogicalUnion(all=[false])\n"
            + "    LogicalProject(EMPNO=[$0], $f1=[||($1, _UTF-16'-1')])\n"
            + "      LogicalTableScan(table=[[" + appName + ", EMP]])\n"
            + "    LogicalProject(EMPNO=[$0], $f1=[||($1, _UTF-16'-2')])\n"
            + "      LogicalTableScan(table=[[" + appName + ", EMP]])\n";
        assertThat(str(root), is(expected));
        checkRowType(root,
            "RecordType(VARCHAR(12) $f1, INTEGER EMPNO)",
            "RecordType(INTEGER EMPNO, VARCHAR(12) $f1)");
    }

    @Test
    public void testRowTypeMultiLevelAlias() {
        final RelBuilder builder = getRelBuilder();
        RelNode root = builder.scan("EMP")
            .as("e")
            .scan("EMP")
            .as("m")
            .scan("DEPT")
            .join(JoinRelType.INNER)
            .join(JoinRelType.INNER)
            .project(builder.field("DEPT", "DEPTNO"),
                builder.field(16),
                builder.field("m", "EMPNO"),
                builder.field("e", "MGR"))
            .as("all")
            .filter(
                builder.call(SqlStdOperatorTable.GREATER_THAN, builder.field("DEPT", "DEPTNO"), builder.literal(100)))
            .project(builder.field("DEPT", "DEPTNO"), builder.field("all", "EMPNO"))
            .build();
        final String expected = "" + "LogicalProject(DEPTNO=[$0], EMPNO=[$2])\n"
            + "  LogicalFilter(condition=[>($0, 100)])\n"
            + "    LogicalProject(DEPTNO=[$16], DEPTNO0=[$16], EMPNO=[$8], MGR=[$3])\n"
            + "      LogicalJoin(condition=[true], joinType=[INNER])\n"
            + "        LogicalTableScan(table=[[" + appName + ", EMP]])\n"
            + "        LogicalJoin(condition=[true], joinType=[INNER])\n"
            + "          LogicalTableScan(table=[[" + appName + ", EMP]])\n"
            + "          LogicalTableScan(table=[[" + appName + ", DEPT]])\n";
        assertThat(str(root), is(expected));

        checkRowType(root,
            "RecordType(INTEGER DEPTNO, INTEGER EMPNO)",
            "RecordType(INTEGER EMPNO, VARCHAR(10) ENAME, VARCHAR(9) JOB, SMALLINT MGR, DATE HIREDATE, DECIMAL(7, 2) SAL, DECIMAL(7, 2) COMM, INTEGER DEPTNO, INTEGER EMPNO, VARCHAR(10) ENAME, VARCHAR(9) JOB, SMALLINT MGR, DATE HIREDATE, DECIMAL(7, 2) SAL, DECIMAL(7, 2) COMM, INTEGER DEPTNO, INTEGER DEPTNO, VARCHAR(14) DNAME, VARCHAR(13) LOC)");
    }

    @Test
    public void testRowTypeAliasPastTop() {
        // Equivalent SQL:
        // SELECT *
        // FROM emp
        // LEFT JOIN dept ON emp.deptno = dept.deptno
        // AND emp.empno = 123
        // AND dept.deptno IS NOT NULL
        final RelBuilder builder = getRelBuilder();
        RelNode root = builder.scan("EMP")
            .scan("DEPT")
            .join(JoinRelType.LEFT,
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field(2, "EMP", "DEPTNO"),
                    builder.field(2, "DEPT", "DEPTNO")),
                builder.call(SqlStdOperatorTable.EQUALS, builder.field(2, "EMP", "EMPNO"), builder.literal(123)))
            .build();
        final String expected = "" + "LogicalJoin(condition=[AND(=($7, $8), =($0, 123))], joinType=[LEFT])\n"
            + "  LogicalTableScan(table=[[" + appName + ", EMP]])\n"
            + "  LogicalTableScan(table=[[" + appName + ", DEPT]])\n";
        assertThat(str(root), is(expected));
        checkRowType(root,
            "RecordType(INTEGER EMPNO, VARCHAR(10) ENAME, VARCHAR(9) JOB, SMALLINT MGR, DATE HIREDATE, DECIMAL(7, 2) SAL, DECIMAL(7, 2) COMM, INTEGER DEPTNO, INTEGER DEPTNO, VARCHAR(14) DNAME, VARCHAR(13) LOC)",
            "RecordType(INTEGER EMPNO, VARCHAR(10) ENAME, VARCHAR(9) JOB, SMALLINT MGR, DATE HIREDATE, DECIMAL(7, 2) SAL, DECIMAL(7, 2) COMM, INTEGER DEPTNO, INTEGER DEPTNO, VARCHAR(14) DNAME, VARCHAR(13) LOC)");
    }

    @Test
    public void testRowTypeAliasPastTop2() {
        // Equivalent SQL:
        // SELECT t1.EMPNO, t2.EMPNO, t3.DEPTNO
        // FROM emp t1
        // INNER JOIN emp t2 ON t1.EMPNO = t2.EMPNO
        // INNER JOIN dept t3 ON t1.DEPTNO = t3.DEPTNO
        // AND t2.JOB != t3.LOC
        final RelBuilder builder = getRelBuilder();
        RelNode root = builder.scan("EMP")
            .as("t1")
            .scan("EMP")
            .as("t2")
            .join(JoinRelType.INNER, builder.equals(builder.field(2, "t1", "EMPNO"), builder.field(2, "t2", "EMPNO")))
            .scan("DEPT")
            .as("t3")
            .join(JoinRelType.INNER,
                builder.equals(builder.field(2, "t1", "DEPTNO"), builder.field(2, "t3", "DEPTNO")),
                builder.not(builder.equals(builder.field(2, "t2", "JOB"), builder.field(2, "t3", "LOC"))))
            .project(builder.field("t1", "EMPNO"), builder.field("t2", "EMPNO"), builder.field("t3", "DEPTNO"))
            .build();
        // Cols:
        // 0-7 EMP as t1
        // 8-15 EMP as t2
        // 16-18 DEPT as t3
        final String expected = "" + "LogicalProject(EMPNO=[$0], EMPNO0=[$8], DEPTNO=[$16])\n"
            + "  LogicalJoin(condition=[AND(=($7, $16), <>($10, $18))], joinType=[INNER])\n"
            + "    LogicalJoin(condition=[=($0, $8)], joinType=[INNER])\n"
            + "      LogicalTableScan(table=[[" + appName + ", EMP]])\n"
            + "      LogicalTableScan(table=[[" + appName + ", EMP]])\n"
            + "    LogicalTableScan(table=[[" + appName + ", DEPT]])\n";
        assertThat(str(root), is(expected));
        checkRowType(root,
            "RecordType(INTEGER EMPNO, INTEGER EMPNO, INTEGER DEPTNO)",
            "RecordType(INTEGER EMPNO, VARCHAR(10) ENAME, VARCHAR(9) JOB, SMALLINT MGR, DATE HIREDATE, DECIMAL(7, 2) SAL, DECIMAL(7, 2) COMM, INTEGER DEPTNO, INTEGER EMPNO, VARCHAR(10) ENAME, VARCHAR(9) JOB, SMALLINT MGR, DATE HIREDATE, DECIMAL(7, 2) SAL, DECIMAL(7, 2) COMM, INTEGER DEPTNO, INTEGER DEPTNO, VARCHAR(14) DNAME, VARCHAR(13) LOC)");
    }

    @Test
    public void testFilter() {
        // Equivalent SQL:
        // SELECT *
        // FROM emp
        // WHERE deptno = 20
        final RelBuilder builder = getRelBuilder();
        RelNode root = builder.scan("EMP").filter(builder.equals(builder.field("DEPTNO"), builder.literal(20))).build();
        assertThat(str(root),
            is("LogicalFilter(condition=[=($7, 20)])\n"
                + "  LogicalTableScan(table=[[" + appName + ", EMP]])\n"));

        final ExtractionResult er = ConditionExtractor.partitioningConditionFrom(root).extract();

        final ConditionResult condition = er.conditionOf(appName, "EMP").first();

        checkCondition(root, condition, "=($7, 20)");

        final Label rootLabel = er.getRootLabel();
        System.out.println(rootLabel.toString());
    }

    @Test
    public void testScanFilterIsNull() {
        // Equivalent SQL:
        // SELECT *
        // FROM emp
        // WHERE deptno IS NULL
        final RelBuilder builder = getRelBuilder();
        RelNode root = builder.scan("EMP")
            .filter(builder.isNull(builder.field("DEPTNO")))
            .build();
        assertThat(str(root),
            is("LogicalFilter(condition=[IS NULL($7)])\n"
                + "  LogicalTableScan(table=[[" + appName + ", EMP]])\n"));

        final ExtractionResult er = ConditionExtractor.partitioningConditionFrom(root).extract();

        final ConditionResult condition = er.conditionOf(appName, "EMP").first();

        checkCondition(root, condition, "IS NULL($7)");

        final Label rootLabel = er.getRootLabel();
        System.out.println(rootLabel.toString());
    }

    @Test
    public void testScanFilterOr() {
        // Equivalent SQL:
        // SELECT *
        // FROM emp
        // WHERE (deptno = 20 OR comm IS NULL) AND mgr IS NOT NULL
        final RelBuilder builder = getRelBuilder();
        RelNode root = builder.scan("EMP")
            .filter(builder.call(SqlStdOperatorTable.OR,
                builder.call(SqlStdOperatorTable.EQUALS, builder.field("DEPTNO"), builder.literal(20)),
                builder.isNull(builder.field(6))), builder.isNotNull(builder.field(3)))
            .build();
        assertThat(str(root),
            is("LogicalFilter(condition=[AND(OR(=($7, 20), IS NULL($6)), IS NOT NULL($3))])\n"
                + "  LogicalTableScan(table=[[" + appName + ", EMP]])\n"));

        final ExtractionResult er = ConditionExtractor.partitioningConditionFrom(root).extract();

        final ConditionResult condition = er.conditionOf(appName, "EMP").first();

        checkCondition(root, condition, "OR(=($7, 20), IS NULL($6))");

        final ExtractionResult er1 = ConditionExtractor.predicateFrom(root).extract();

        final ConditionResult condition1 = er1.conditionOf(appName, "EMP").first();

        checkCondition(root, condition1, "AND(IS NOT NULL($3), OR(=($7, 20), IS NULL($6)))");

        final Label rootLabel = er.getRootLabel();
        System.out.println(rootLabel.toString());
    }

    @Test
    public void testScanFilterOr2() {
        // Equivalent SQL:
        // SELECT *
        // FROM emp
        // WHERE deptno > 20 OR deptno > 20
        // simplifies to
        // SELECT *
        // FROM emp
        // WHERE deptno = 20
        final RelBuilder builder = getRelBuilder();
        RelNode root = builder.scan("EMP")
            .filter(builder.call(SqlStdOperatorTable.OR,
                builder.call(SqlStdOperatorTable.GREATER_THAN, builder.field("DEPTNO"), builder.literal(20)),
                builder.call(SqlStdOperatorTable.GREATER_THAN, builder.field("DEPTNO"), builder.literal(20))))
            .build();
        assertThat(str(root),
            is("LogicalFilter(condition=[OR(>($7, 20), >($7, 20))])\n"
                + "  LogicalTableScan(table=[[" + appName + ", EMP]])\n"));

        final ExtractionResult er = ConditionExtractor.partitioningConditionFrom(root).extract();

        final ConditionResult condition = er.conditionOf(appName, "EMP").first();

        checkCondition(root, condition, ">($7, 20)");

        final Label rootLabel = er.getRootLabel();
        System.out.println(rootLabel.toString());
    }

    @Test
    public void testScanFilterAndFalse() {
        // Equivalent SQL:
        //   SELECT *
        //   FROM emp
        //   WHERE deptno = 20 AND FALSE
        final RelBuilder builder = getRelBuilder();
        RelNode root =
            builder.scan("EMP")
                .filter(
                    builder.call(SqlStdOperatorTable.EQUALS,
                        builder.field("DEPTNO"),
                        builder.literal(20)),
                    builder.literal(false))
                .build();
        final String plan = "LogicalFilter(condition=[AND(=($7, 20), false)])\n"
            + "  LogicalTableScan(table=[[" + appName + ", EMP]])\n";
        assertThat(str(root), is(plan));

        final ExtractionResult er = ConditionExtractor.partitioningConditionFrom(root).extract();

        final ConditionResult condition = er.conditionOf(appName, "EMP").first();

        checkCondition(root, condition, "false");

        final Label rootLabel = er.getRootLabel();
        System.out.println(rootLabel.toString());
    }

    @Test
    public void testScanFilterAndTrue() {
        // Equivalent SQL:
        //   SELECT *
        //   FROM emp
        //   WHERE deptno = 20 AND TRUE
        final RelBuilder builder = getRelBuilder();
        RelNode root =
            builder.scan("EMP")
                .filter(
                    builder.call(SqlStdOperatorTable.GREATER_THAN,
                        builder.field("DEPTNO"),
                        builder.literal(20)),
                    builder.literal(true))
                .build();
        final String plan = "LogicalFilter(condition=[AND(>($7, 20), true)])\n"
            + "  LogicalTableScan(table=[[" + appName + ", EMP]])\n";
        assertThat(str(root), is(plan));

        final ExtractionResult er = ConditionExtractor.partitioningConditionFrom(root).extract();

        final ConditionResult condition = er.conditionOf(appName, "EMP").first();

        checkCondition(root, condition, ">($7, 20)");

        final Label rootLabel = er.getRootLabel();
        System.out.println(rootLabel.toString());
    }

    @Test
    public void testScanFilterOrFalse() {
        // Equivalent SQL:
        //   SELECT *
        //   FROM emp
        //   WHERE deptno = 20 AND FALSE
        final RelBuilder builder = getRelBuilder();
        RelNode root =
            builder.scan("EMP")
                .filter(builder.call(SqlStdOperatorTable.OR,
                    builder.call(SqlStdOperatorTable.EQUALS,
                        builder.field("DEPTNO"),
                        builder.literal(20)),
                    builder.call(SqlStdOperatorTable.EQUALS,
                        builder.literal(12),
                        builder.literal(20))
                ))
                .build();
        final String plan = "LogicalFilter(condition=[OR(=($7, 20), =(12, 20))])\n"
            + "  LogicalTableScan(table=[[" + appName + ", EMP]])\n";
        assertThat(str(root), is(plan));

        final ExtractionResult er = ConditionExtractor.partitioningConditionFrom(root).extract();

        final ConditionResult condition = er.conditionOf(appName, "EMP").first();

        checkCondition(root, condition, "=($7, 20)");

        final Label rootLabel = er.getRootLabel();
        System.out.println(rootLabel.toString());
    }

    @Test
    public void testScanAggregate1() {
        // Equivalent SQL:
        //   SELECT c, "mid" as l, s
        //   FROM (
        //     SELECT COUNT(*) AS c, SUM(mgr + 1) AS s
        //     FROM emp
        //     WHERE deptno = 20
        //     GROUP BY ename, hiredate + mgr
        //     HAVING c > 10
        //   ) t
        final RelBuilder builder = getRelBuilder();
        RelNode root =
            builder.scan("EMP")
                .filter(builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field("DEPTNO"),
                    builder.literal(20L)))
                .aggregate(
                    builder.groupKey(builder.field(1),
                        builder.call(SqlStdOperatorTable.PLUS,
                            builder.field(4),
                            builder.field(3)),
                        builder.field(1)),
                    builder.aggregateCall(SqlStdOperatorTable.COUNT, false, false,
                        null, "C"),
                    builder.aggregateCall(SqlStdOperatorTable.SUM, false, false,
                        null, "S",
                        builder.call(SqlStdOperatorTable.PLUS, builder.field(3),
                            builder.literal(1))))
                .filter(builder.call(SqlStdOperatorTable.GREATER_THAN,
                    builder.field(0),
                    builder.literal(10L)))
                .project(builder.field(0),
                    builder.alias(builder.literal("mid"), "l"),
                    builder.field(1))
                .build();
        assertThat(str(root),
            is(""
                + "LogicalProject(ENAME=[$0], l=[_UTF-16'mid'], $f8=[$1])\n"
                + "  LogicalFilter(condition=[>($0, 10)])\n"
                + "    LogicalAggregate(group=[{1, 8}], C=[COUNT()], S=[SUM($9)])\n"
                + "      LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4], SAL=[$5], COMM=[$6], DEPTNO=[$7], $f8=[+($4, $3)], $f9=[+($3, 1)])\n"
                + "        LogicalFilter(condition=[=($7, 20)])\n"
                + "          LogicalTableScan(table=[[" + appName + ", EMP]])\n"));

        final ExtractionResult er = ConditionExtractor.partitioningConditionFrom(root).extract();

        final ConditionResult condition = er.conditionOf(appName, "EMP").first();

        checkCondition(root, condition, "=($7, 20)");

        final Label rootLabel = er.getRootLabel();
        System.out.println(rootLabel.toString());
    }

    @Test
    public void testScanJoin() {
        // Equivalent SQL:
        //   SELECT *
        //   FROM (SELECT * FROM emp WHERE comm IS NULL)
        //   JOIN dept ON emp.deptno = dept.deptno
        final RelBuilder builder = getRelBuilder();
        RelNode root =
            builder.scan("EMP")
                .filter(
                    builder.call(SqlStdOperatorTable.IS_NULL,
                        builder.field("COMM")))
                .scan("DEPT")
                .join(JoinRelType.INNER,
                    builder.call(SqlStdOperatorTable.EQUALS,
                        builder.field(2, 0, "DEPTNO"),
                        builder.field(2, 1, "DEPTNO")))
                .build();
        final String expected = ""
            + "LogicalJoin(condition=[=($7, $8)], joinType=[INNER])\n"
            + "  LogicalFilter(condition=[IS NULL($6)])\n"
            + "    LogicalTableScan(table=[[" + appName + ", EMP]])\n"
            + "  LogicalTableScan(table=[[" + appName + ", DEPT]])\n";
        assertThat(str(root), is(expected));

        final ExtractionResult er = ConditionExtractor.partitioningConditionFrom(root).extract();

        checkCondition(root, er.conditionOf(appName, "EMP").first(), "IS NULL($6)");

        final Label rootLabel = er.getRootLabel();
        System.out.println(rootLabel.toString());
    }

    @Test
    public void testScanJoin2() {
        // Equivalent SQL:
        //   SELECT *
        //   FROM emp
        //   LEFT JOIN dept ON emp.deptno = dept.deptno
        //     AND emp.empno = 123
        //     AND dept.deptno IS NOT NULL
        final RelBuilder builder = getRelBuilder();
        RelNode root =
            builder.scan("EMP")
                .scan("DEPT")
                .join(JoinRelType.LEFT,
                    builder.call(SqlStdOperatorTable.EQUALS,
                        builder.field(2, 0, "DEPTNO"),
                        builder.field(2, 1, "DEPTNO")),
                    builder.call(SqlStdOperatorTable.EQUALS,
                        builder.field(2, 0, "EMPNO"),
                        builder.literal(123)),
                    builder.call(SqlStdOperatorTable.IS_NOT_NULL,
                        builder.field(2, 1, "DNAME")))
                .build();
        final String expected = ""
            + "LogicalJoin(condition=[AND(=($7, $8), =($0, 123), IS NOT NULL($9))], joinType=[LEFT])\n"
            + "  LogicalTableScan(table=[[" + appName + ", EMP]])\n"
            + "  LogicalTableScan(table=[[" + appName + ", DEPT]])\n";
        assertThat(str(root), is(expected));

        final ExtractionResult er = ConditionExtractor.partitioningConditionFrom(root).extract();

        checkCondition(root, er.conditionOf(appName, "EMP").first(), "true");
        checkCondition(root, er.conditionOf(appName, "DEPT").first(), "true");

        final Label rootLabel = er.getRootLabel();
        System.out.println(rootLabel.toString());
    }

    @Test
    public void testScanJoin3() {
        // Equivalent SQL:
        //   SELECT *
        //   FROM emp
        //   JOIN (
        //      SELECT 1 AS deptn
        //      FROM dept
        //      WHERE DEPTNO IS NULL
        //      ) t
        //   WHERE emp.deptno = t.deptno
        //     AND emp.empno = 123
        final RelBuilder builder = getRelBuilder();
        RelNode root = builder.scan("EMP")
            .scan("DEPT")
            .filter(builder.call(SqlStdOperatorTable.IS_NULL,
                builder.field("DEPTNO")))
            .project(builder.alias(builder.literal(1L), "deptno"))
            .join(JoinRelType.INNER)
            .filter(
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field(7),
                    builder.field(8)),
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field("EMPNO"),
                    builder.literal(123)))
            .build();
        // Note that "dept.deptno IS NOT NULL" has been simplified away.
        final String expected = ""
            + "LogicalFilter(condition=[AND(=($7, $8), =($0, 123))])\n"
            + "  LogicalJoin(condition=[true], joinType=[INNER])\n"
            + "    LogicalTableScan(table=[[" + appName + ", EMP]])\n"
            + "    LogicalProject(deptno=[1])\n"
            + "      LogicalFilter(condition=[IS NULL($0)])\n"
            + "        LogicalTableScan(table=[[" + appName + ", DEPT]])\n";
        assertThat(str(root), is(expected));

        final ExtractionResult er = ConditionExtractor.partitioningConditionFrom(root).extract();

        checkCondition(root, er.conditionOf(appName, "EMP").first(), "=($0, 123)");
        checkCondition(root, er.conditionOf(appName, "DEPT").first(), "IS NULL($0)");

        final Label rootLabel = er.getRootLabel();
        System.out.println(rootLabel.toString());
    }

    @Test
    public void testScanJoin4() {
        // Equivalent SQL:
        //   SELECT *
        //   FROM emp, dept, bonus
        //   WHERE emp.deptno = dept.deptno
        //     AND bonus.ename = emp.ename
        final RelBuilder builder = getRelBuilder();
        RelNode root = builder.scan("EMP")
            .scan("DEPT")
            .join(JoinRelType.INNER)
            .scan("BONUS")
            .join(JoinRelType.INNER)
            .filter(
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field(7),
                    builder.field(8)),
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field(1),
                    builder.field(11)))
            .build();
        // Note that "dept.deptno IS NOT NULL" has been simplified away.
        final String expected = ""
            + "LogicalFilter(condition=[AND(=($7, $8), =($1, $11))])\n"
            + "  LogicalJoin(condition=[true], joinType=[INNER])\n"
            + "    LogicalJoin(condition=[true], joinType=[INNER])\n"
            + "      LogicalTableScan(table=[[" + appName + ", EMP]])\n"
            + "      LogicalTableScan(table=[[" + appName + ", DEPT]])\n"
            + "    LogicalTableScan(table=[[" + appName + ", BONUS]])\n";
        assertThat(str(root), is(expected));

        final ExtractionResult er = ConditionExtractor.partitioningConditionFrom(root).extract();

        checkCondition(root, er.conditionOf(appName, "EMP").intersect(), "true");
        checkCondition(root, er.conditionOf(appName, "DEPT").intersect(), "true");
        checkCondition(root, er.conditionOf(appName, "BONUS").intersect(), "true");

        final ExtractionResult ce = ConditionExtractor.columnEquivalenceFrom(root).extract();
        final RelNode join = root.getInput(0);
        final Map<Integer, BitSet> bitSets = ce.conditionOf(join).toColumnEquality();
        assertThat(bitSets.get(1).get(11), is(true));
        assertThat(bitSets.get(11).get(1), is(true));
        assertThat(bitSets.get(7).get(8), is(true));
        assertThat(bitSets.get(8).get(7), is(true));

        final ExtractionResult ce1 = ConditionExtractor.columnEquivalenceFrom(join).extract();
        final Map<Integer, BitSet> bitSets1 = ce1.conditionOf(join).toColumnEquality();
        assertThat(bitSets1.get(1).get(11), is(false));
        assertThat(bitSets1.get(11).get(1), is(false));
        assertThat(bitSets1.get(7).get(8), is(false));
        assertThat(bitSets1.get(8).get(7), is(false));
        final Map<Integer, BitSet> bitSets2 = ce1.conditionOf(join.getInput(0)).toColumnEquality();
        assertThat(bitSets2.get(7).get(8), is(false));
        assertThat(bitSets2.get(8).get(7), is(false));

        final Label rootLabel = er.getRootLabel();
        System.out.println(rootLabel.toString());
    }

    @Test
    public void testScanInnerJoinOnCondition1() {
        // Equivalent SQL:
        //   SELECT *
        //   FROM t1
        //     INNER JOIN t2 ON t1.id = t2.id AND t2.id = 4
        //     INNER JOIN t3 ON t1.id = t3.id
        final RelBuilder builder = getRelBuilder();
        RelNode root = builder.scan("t1")
            .scan("t2")
            .join(JoinRelType.INNER,
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field(2, 0, "id"),
                    builder.field(2, 1, "id")),
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field(2, 1, "id"),
                    builder.literal(4)))
            .scan("t3")
            .join(JoinRelType.INNER,
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field(2, 0, 0),
                    builder.field(2, 1, "id"))
            )
            .build();
        final String expected = ""
            + "LogicalJoin(condition=[=($0, $6)], joinType=[INNER])\n"
            + "  LogicalJoin(condition=[AND(=($0, $3), =($3, 4))], joinType=[INNER])\n"
            + "    LogicalTableScan(table=[[" + appName + ", t1]])\n"
            + "    LogicalTableScan(table=[[" + appName + ", t2]])\n"
            + "  LogicalTableScan(table=[[" + appName + ", t3]])\n";
        assertThat(str(root), is(expected));

        final ExtractionResult er = ConditionExtractor.partitioningConditionFrom(root).extract();

        checkCondition(root, er.conditionOf(appName, "t1").intersect(), "=($0, 4)");
        checkCondition(root, er.conditionOf(appName, "t2").intersect(), "=($0, 4)");
        checkCondition(root, er.conditionOf(appName, "t3").intersect(), "=($0, 4)");

        final RelNode join2 = root;
        final RelNode join1 = root.getInput(0);

        ExtractionResult ce = ConditionExtractor.columnEquivalenceFrom(root).extract();
        Map<Integer, BitSet> bitSets = ce.conditionOf(join2).toColumnEquality();
        assertThat(bitSets.get(0), is(BitSets.of(0, 3, 6)));
        assertThat(bitSets.get(3), is(BitSets.of(0, 3, 6)));
        assertThat(bitSets.get(6), is(BitSets.of(0, 3, 6)));

        bitSets = ce.conditionOf(join1).toColumnEquality();
        assertThat(bitSets.get(0), is(BitSets.of(0, 3)));
        assertThat(bitSets.get(3), is(BitSets.of(0, 3)));

        ce = ConditionExtractor.columnEquivalenceFrom(join1).extract();
        bitSets = ce.conditionOf(join1).toColumnEquality();
        assertThat(bitSets.get(0), is(BitSets.of(0, 3)));
        assertThat(bitSets.get(3), is(BitSets.of(0, 3)));

        final Label rootLabel = er.getRootLabel();
        System.out.println(rootLabel.toString());
    }

    @Test
    public void testScanInnerJoinOnCondition2() {
        // Equivalent SQL:
        //   SELECT *
        //   FROM t1 INNER JOIN t2
        //   WHERE t1.id = t2.id AND t2.id = 4
        final RelBuilder builder = getRelBuilder();
        RelNode root = builder.scan("t1")
            .scan("t2")
            .join(JoinRelType.INNER)
            .filter(builder.call(SqlStdOperatorTable.EQUALS, builder.field(0), builder.field(3)),
                builder.call(SqlStdOperatorTable.EQUALS, builder.field(3), builder.literal(4)))
            .build();
        final String expected = ""
            + "LogicalFilter(condition=[AND(=($0, $3), =($3, 4))])\n"
            + "  LogicalJoin(condition=[true], joinType=[INNER])\n"
            + "    LogicalTableScan(table=[[" + appName + ", t1]])\n"
            + "    LogicalTableScan(table=[[" + appName + ", t2]])\n";
        assertThat(str(root), is(expected));

        final ExtractionResult er = ConditionExtractor.partitioningConditionFrom(root).extract();

        checkCondition(root, er.conditionOf(appName, "t1").intersect(), "=($0, 4)");
        checkCondition(root, er.conditionOf(appName, "t2").intersect(), "=($0, 4)");

        final RelNode join = root.getInput(0);

        ExtractionResult ce = ConditionExtractor.columnEquivalenceFrom(root).extract();
        Map<Integer, BitSet> bitSets = ce.conditionOf(join).toColumnEquality();
        assertThat(bitSets.get(0), is(BitSets.of(0, 3)));
        assertThat(bitSets.get(3), is(BitSets.of(0, 3)));

        final Label rootLabel = er.getRootLabel();
        System.out.println(rootLabel.toString());
    }

    @Test
    public void testScanLeftJoinOnCondition1() {
        // Equivalent SQL:
        //   SELECT *
        //   FROM t1
        //     LEFT JOIN t2 ON t1.id = t2.id AND t1.id = 4 AND t2.id = 5
        //     LEFT JOIN t3 ON t1.id = t3.id AND t3.id = 6
        final RelBuilder builder = getRelBuilder();
        RelNode root = builder.scan("t1")
            .scan("t2")
            .join(JoinRelType.LEFT,
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field(2, 0, "id"),
                    builder.field(2, 1, "id")),
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field(2, 0, "id"),
                    builder.literal(4)),
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field(2, 1, "id"),
                    builder.literal(5))
            )
            .scan("t3")
            .join(JoinRelType.LEFT,
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field(2, 0, 0),
                    builder.field(2, 1, "id")),
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field(2, 1, "id"),
                    builder.literal(6))
            )
            .build();
        final String expected = ""
            + "LogicalJoin(condition=[AND(=($0, $6), =($6, 6))], joinType=[LEFT])\n"
            + "  LogicalJoin(condition=[AND(=($0, $3), =($0, 4), =($3, 5))], joinType=[LEFT])\n"
            + "    LogicalTableScan(table=[[" + appName + ", t1]])\n"
            + "    LogicalTableScan(table=[[" + appName + ", t2]])\n"
            + "  LogicalTableScan(table=[[" + appName + ", t3]])\n";
        assertThat(str(root), is(expected));

        final ExtractionResult er = ConditionExtractor.partitioningConditionFrom(root).extract();

        checkCondition(root, er.conditionOf(appName, "t1").intersect(), "true");
        checkCondition(root, er.conditionOf(appName, "t2").intersect(), "AND(=($0, 4), =($0, 5))");
        checkCondition(root, er.conditionOf(appName, "t3").intersect(), "=($0, 6)");

        final RelNode join2 = root;
        final RelNode join1 = root.getInput(0);

        ExtractionResult ce = ConditionExtractor.columnEquivalenceFrom(root).extract();
        Map<Integer, BitSet> bitSets = ce.conditionOf(join2).toColumnEquality();
        assertThat(bitSets.get(0), is(BitSets.of(0, 3, 6)));
        assertThat(bitSets.get(3), is(BitSets.of(0, 3, 6)));
        assertThat(bitSets.get(6), is(BitSets.of(0, 3, 6)));

        bitSets = ce.conditionOf(join1).toColumnEquality();
        assertThat(bitSets.get(0), is(BitSets.of(0, 3)));
        assertThat(bitSets.get(3), is(BitSets.of(0, 3)));

        ce = ConditionExtractor.columnEquivalenceFrom(join1).extract();
        bitSets = ce.conditionOf(join1).toColumnEquality();
        assertThat(bitSets.get(0), is(BitSets.of(0, 3)));
        assertThat(bitSets.get(3), is(BitSets.of(0, 3)));

        final Label rootLabel = er.getRootLabel();
        System.out.println(rootLabel.toString());
    }

    @Test
    public void testScanLeftJoinOnCondition2() {
        // Equivalent SQL:
        //   SELECT *
        //   FROM t1
        //     LEFT JOIN t2 ON t1.id = t2.id AND t1.id = 4 AND t2.id = 5
        //     LEFT JOIN t3 ON t2.id = t3.id
        final RelBuilder builder = getRelBuilder();
        RelNode root = builder.scan("t1")
            .scan("t2")
            .join(JoinRelType.LEFT,
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field(2, 0, "id"),
                    builder.field(2, 1, "id")),
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field(2, 0, "id"),
                    builder.literal(4)),
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field(2, 1, "id"),
                    builder.literal(5))
            )
            .scan("t3")
            .join(JoinRelType.LEFT,
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field(2, 0, 3),
                    builder.field(2, 1, "id"))
            )
            .build();
        final String expected = ""
            + "LogicalJoin(condition=[=($3, $6)], joinType=[LEFT])\n"
            + "  LogicalJoin(condition=[AND(=($0, $3), =($0, 4), =($3, 5))], joinType=[LEFT])\n"
            + "    LogicalTableScan(table=[[" + appName + ", t1]])\n"
            + "    LogicalTableScan(table=[[" + appName + ", t2]])\n"
            + "  LogicalTableScan(table=[[" + appName + ", t3]])\n";
        assertThat(str(root), is(expected));

        final ExtractionResult er = ConditionExtractor.partitioningConditionFrom(root).extract();

        checkCondition(root, er.conditionOf(appName, "t1").intersect(), "true");
        checkCondition(root, er.conditionOf(appName, "t2").intersect(), "AND(=($0, 4), =($0, 5))");
        checkCondition(root, er.conditionOf(appName, "t3").intersect(), "AND(=($0, 4), =($0, 5))");

        final RelNode join2 = root;
        final RelNode join1 = root.getInput(0);

        ExtractionResult ce = ConditionExtractor.columnEquivalenceFrom(root).extract();
        Map<Integer, BitSet> bitSets = ce.conditionOf(join2).toColumnEquality();
        assertThat(bitSets.get(0), is(BitSets.of(0, 3, 6)));
        assertThat(bitSets.get(3), is(BitSets.of(0, 3, 6)));
        assertThat(bitSets.get(6), is(BitSets.of(0, 3, 6)));

        bitSets = ce.conditionOf(join1).toColumnEquality();
        assertThat(bitSets.get(0), is(BitSets.of(0, 3)));
        assertThat(bitSets.get(3), is(BitSets.of(0, 3)));

        ce = ConditionExtractor.columnEquivalenceFrom(join1).extract();
        bitSets = ce.conditionOf(join1).toColumnEquality();
        assertThat(bitSets.get(0), is(BitSets.of(0, 3)));
        assertThat(bitSets.get(3), is(BitSets.of(0, 3)));

        final Label rootLabel = er.getRootLabel();
        System.out.println(rootLabel.toString());
    }

    @Test
    public void testScanLeftJoinOnCondition3() {
        // Equivalent SQL:
        //   SELECT *
        //   FROM t1
        //     LEFT JOIN t2 ON t1.id = 4 AND t2.id = 5
        //     LEFT JOIN t3 ON t2.id = t3.id AND t1.id = t2.id
        final RelBuilder builder = getRelBuilder();
        RelNode root = builder.scan("t1")
            .scan("t2")
            .join(JoinRelType.LEFT,
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field(2, 0, "id"),
                    builder.literal(4)),
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field(2, 1, "id"),
                    builder.literal(5))
            )
            .scan("t3")
            .join(JoinRelType.LEFT,
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field(2, 0, 3),
                    builder.field(2, 1, "id")),
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field(2, 0, "id"),
                    builder.field(2, 0, 3))
            )
            .build();
        final String expected = ""
            + "LogicalJoin(condition=[AND(=($3, $6), =($0, $3))], joinType=[LEFT])\n"
            + "  LogicalJoin(condition=[AND(=($0, 4), =($3, 5))], joinType=[LEFT])\n"
            + "    LogicalTableScan(table=[[" + appName + ", t1]])\n"
            + "    LogicalTableScan(table=[[" + appName + ", t2]])\n"
            + "  LogicalTableScan(table=[[" + appName + ", t3]])\n";
        assertThat(str(root), is(expected));

        final ExtractionResult er = ConditionExtractor.partitioningConditionFrom(root).extract();

        checkCondition(root, er.conditionOf(appName, "t1").intersect(), "true");
        checkCondition(root, er.conditionOf(appName, "t2").intersect(), "=($0, 5)");
        checkCondition(root, er.conditionOf(appName, "t3").intersect(), "=($0, 5)");

        final RelNode join2 = root;
        final RelNode join1 = root.getInput(0);

        ExtractionResult ce = ConditionExtractor.columnEquivalenceFrom(root).extract();
        Map<Integer, BitSet> bitSets = ce.conditionOf(join2).toColumnEquality();
        assertThat(bitSets.get(0), is(BitSets.of(0, 6)));
        assertThat(bitSets.get(3), is(BitSets.of(3, 6)));
        assertThat(bitSets.get(6), is(BitSets.of(0, 3, 6)));

        bitSets = ce.conditionOf(join1).toColumnEquality();
        assertThat(bitSets.get(0), is(BitSets.of(0)));
        assertThat(bitSets.get(3), is(BitSets.of(3)));

        ce = ConditionExtractor.columnEquivalenceFrom(join1).extract();
        bitSets = ce.conditionOf(join1).toColumnEquality();
        assertThat(bitSets.get(0), is(BitSets.of(0)));
        assertThat(bitSets.get(3), is(BitSets.of(3)));

        final Label rootLabel = er.getRootLabel();
        System.out.println(rootLabel.toString());
    }

    @Test
    public void testScanLeftJoinOnCondition4() {
        // Equivalent SQL:
        //   SELECT *
        //   FROM t3 LEFT JOIN (
        //     t1 LEFT JOIN t2 ON t1.id = 4 AND t2.id = 5)
        //     ON t2.id = t3.id AND t1.id = t2.id
        final RelBuilder builder = getRelBuilder();
        RelNode root = builder.scan("t3")
            .scan("t1")
            .scan("t2")
            .join(JoinRelType.LEFT,
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field(2, 0, "id"),
                    builder.literal(4)),
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field(2, 1, "id"),
                    builder.literal(5))
            )
            .join(JoinRelType.LEFT,
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field(2, 1, 3),
                    builder.field(2, 0, "id")),
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field(2, 1, 0),
                    builder.field(2, 1, 3))
            )
            .build();
        final String expected = ""
            + "LogicalJoin(condition=[AND(=($6, $0), =($3, $6))], joinType=[LEFT])\n"
            + "  LogicalTableScan(table=[[" + appName + ", t3]])\n"
            + "  LogicalJoin(condition=[AND(=($0, 4), =($3, 5))], joinType=[LEFT])\n"
            + "    LogicalTableScan(table=[[" + appName + ", t1]])\n"
            + "    LogicalTableScan(table=[[" + appName + ", t2]])\n";
        assertThat(str(root), is(expected));

        final ExtractionResult er = ConditionExtractor.partitioningConditionFrom(root).extract();

        checkCondition(root, er.conditionOf(appName, "t1").intersect(), "true");
        checkCondition(root, er.conditionOf(appName, "t2").intersect(), "=($0, 5)");
        checkCondition(root, er.conditionOf(appName, "t3").intersect(), "true");

        final RelNode join2 = root;
        final RelNode join1 = root.getInput(1);

        ExtractionResult ce = ConditionExtractor.columnEquivalenceFrom(root).extract();
        Map<Integer, BitSet> bitSets = ce.conditionOf(join2).toColumnEquality();
        assertThat(bitSets.get(0), is(BitSets.of(0, 3, 6)));
        assertThat(bitSets.get(3), is(BitSets.of(0, 3, 6)));
        assertThat(bitSets.get(6), is(BitSets.of(0, 3, 6)));

        bitSets = ce.conditionOf(join1).toColumnEquality();
        assertThat(bitSets.get(0), is(BitSets.of(0, 3)));
        assertThat(bitSets.get(3), is(BitSets.of(0, 3)));

        ce = ConditionExtractor.columnEquivalenceFrom(join1).extract();
        bitSets = ce.conditionOf(join1).toColumnEquality();
        assertThat(bitSets.get(0), is(BitSets.of(0)));
        assertThat(bitSets.get(3), is(BitSets.of(3)));

        final Label rootLabel = er.getRootLabel();
        System.out.println(rootLabel.toString());
    }

    @Test
    public void testScanLeftJoinOnCondition5() {
        // Equivalent SQL:
        //   SELECT *
        //   FROM t1
        //     LEFT JOIN t2 ON t1.id = 4 AND t2.id = 5
        //     LEFT JOIN t3 ON t2.id = t3.id AND t1.id = t3.id
        //     JOIN t4 ON t3.id = t4.id
        final RelBuilder builder = getRelBuilder();
        RelNode root = builder.scan("t1")
            .scan("t2")
            .join(JoinRelType.LEFT,
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field(2, 0, "id"),
                    builder.literal(4)),
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field(2, 1, "id"),
                    builder.literal(5))
            )
            .scan("t3")
            .join(JoinRelType.LEFT,
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field(2, 0, 3),
                    builder.field(2, 1, 0)),
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field(2, 0, 0),
                    builder.field(2, 1, 0))
            )
            .scan("t4")
            .join(JoinRelType.INNER,
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field(2, 0, 6),
                    builder.field(2, 1, 0))
            )
            .build();
        final String expected = ""
            + "LogicalJoin(condition=[=($6, $9)], joinType=[INNER])\n"
            + "  LogicalJoin(condition=[AND(=($3, $6), =($0, $6))], joinType=[LEFT])\n"
            + "    LogicalJoin(condition=[AND(=($0, 4), =($3, 5))], joinType=[LEFT])\n"
            + "      LogicalTableScan(table=[[" + appName + ", t1]])\n"
            + "      LogicalTableScan(table=[[" + appName + ", t2]])\n"
            + "    LogicalTableScan(table=[[" + appName + ", t3]])\n"
            + "  LogicalTableScan(table=[[" + appName + ", t4]])\n";
        assertThat(str(root), is(expected));

        final ExtractionResult er = ConditionExtractor.partitioningConditionFrom(root).extract();

        checkCondition(root, er.conditionOf(appName, "t1").intersect(), "true");
        checkCondition(root, er.conditionOf(appName, "t2").intersect(), "=($0, 5)");
        checkCondition(root, er.conditionOf(appName, "t3").intersect(), "=($0, 5)");
        checkCondition(root, er.conditionOf(appName, "t4").intersect(), "=($0, 5)");

        final RelNode join3 = root;
        final RelNode join2 = root.getInput(0);
        final RelNode join1 = join2.getInput(0);

        ExtractionResult ce = ConditionExtractor.columnEquivalenceFrom(root).extract();
        Map<Integer, BitSet> bitSets = ce.conditionOf(join3).toColumnEquality();
        assertThat(bitSets.get(0), is(BitSets.of(0, 6, 9)));
        assertThat(bitSets.get(3), is(BitSets.of(3, 6, 9)));
        assertThat(bitSets.get(6), is(BitSets.of(0, 3, 6, 9)));
        assertThat(bitSets.get(9), is(BitSets.of(0, 3, 6, 9)));

        bitSets = ce.conditionOf(join2).toColumnEquality();
        assertThat(bitSets.get(0), is(BitSets.of(0, 6)));
        assertThat(bitSets.get(3), is(BitSets.of(3, 6)));

        bitSets = ce.conditionOf(join1).toColumnEquality();
        assertThat(bitSets.get(0), is(BitSets.of(0)));
        assertThat(bitSets.get(3), is(BitSets.of(3)));

        ce = ConditionExtractor.columnEquivalenceFrom(join2).extract();
        bitSets = ce.conditionOf(join2).toColumnEquality();
        assertThat(bitSets.get(0), is(BitSets.of(0, 6)));
        assertThat(bitSets.get(3), is(BitSets.of(3, 6)));

        final Label rootLabel = er.getRootLabel();
        System.out.println(rootLabel.toString());
    }

    @Test
    public void testScanLeftJoinOnCondition6() {
        // Equivalent SQL:
        //   SELECT *
        //   FROM t1 LEFT JOIN t2 ON t1.id = t2.id
        //   WHERE t2.id = 6
        final RelBuilder builder = getRelBuilder();
        RelNode root = builder.scan("t1")
            .scan("t2")
            .join(JoinRelType.LEFT,
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field(2, 0, 0),
                    builder.field(2, 1, 0)))
            .filter(builder.call(SqlStdOperatorTable.EQUALS,
                builder.field(3),
                builder.literal(6)))
            .build();
        final String expected = ""
            + "LogicalFilter(condition=[=($3, 6)])\n"
            + "  LogicalJoin(condition=[=($0, $3)], joinType=[LEFT])\n"
            + "    LogicalTableScan(table=[[" + appName + ", t1]])\n"
            + "    LogicalTableScan(table=[[" + appName + ", t2]])\n";
        assertThat(str(root), is(expected));

        final ExtractionResult er = ConditionExtractor.partitioningConditionFrom(root).extract();

        checkCondition(root, er.conditionOf(appName, "t1").intersect(), "=($0, 6)");
        checkCondition(root, er.conditionOf(appName, "t2").intersect(), "=($0, 6)");

        final RelNode join = root.getInput(0);

        ExtractionResult ce = ConditionExtractor.columnEquivalenceFrom(root).extract();
        Map<Integer, BitSet> bitSets = ce.conditionOf(join).toColumnEquality();
        assertThat(bitSets.get(0), is(BitSets.of(0, 3)));
        assertThat(bitSets.get(3), is(BitSets.of(0, 3)));

        final Label rootLabel = er.getRootLabel();
        System.out.println(rootLabel.toString());
    }

    @Test
    public void testScanLeftJoinOnCondition7() {
        // Equivalent SQL:
        //   SELECT *
        //   FROM t1 LEFT JOIN t2 ON t1.id = t2.id
        //   WHERE t1.id = 6
        final RelBuilder builder = getRelBuilder();
        RelNode root = builder.scan("t1")
            .scan("t2")
            .join(JoinRelType.LEFT,
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field(2, 0, 0),
                    builder.field(2, 1, 0)))
            .filter(builder.call(SqlStdOperatorTable.EQUALS,
                builder.field(0),
                builder.literal(6)))
            .build();
        final String expected = ""
            + "LogicalFilter(condition=[=($0, 6)])\n"
            + "  LogicalJoin(condition=[=($0, $3)], joinType=[LEFT])\n"
            + "    LogicalTableScan(table=[[" + appName + ", t1]])\n"
            + "    LogicalTableScan(table=[[" + appName + ", t2]])\n";
        assertThat(str(root), is(expected));

        final ExtractionResult er = ConditionExtractor.partitioningConditionFrom(root).extract();

        checkCondition(root, er.conditionOf(appName, "t1").intersect(), "=($0, 6)");
        checkCondition(root, er.conditionOf(appName, "t2").intersect(), "=($0, 6)");

        final RelNode join = root.getInput(0);

        ExtractionResult ce = ConditionExtractor.columnEquivalenceFrom(root).extract();
        Map<Integer, BitSet> bitSets = ce.conditionOf(join).toColumnEquality();
        assertThat(bitSets.get(0), is(BitSets.of(0, 3)));
        assertThat(bitSets.get(3), is(BitSets.of(0, 3)));

        final Label rootLabel = er.getRootLabel();
        System.out.println(rootLabel.toString());
    }

    @Test
    public void testScanLeftJoinOnCondition8() {
        // Equivalent SQL:
        //   SELECT *
        //   FROM t1 LEFT JOIN t2 ON t1.id = t2.id AND t1.id = 6
        final RelBuilder builder = getRelBuilder();
        RelNode root = builder.scan("t1")
            .scan("t2")
            .join(JoinRelType.LEFT,
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field(2, 0, 0),
                    builder.field(2, 1, 0)),
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field(2, 0, 0),
                    builder.literal(6)))
            .build();
        final String expected = ""
            + "LogicalJoin(condition=[AND(=($0, $3), =($0, 6))], joinType=[LEFT])\n"
            + "  LogicalTableScan(table=[[" + appName + ", t1]])\n"
            + "  LogicalTableScan(table=[[" + appName + ", t2]])\n";
        assertThat(str(root), is(expected));

        final ExtractionResult er = ConditionExtractor.partitioningConditionFrom(root).extract();

        checkCondition(root, er.conditionOf(appName, "t1").intersect(), "true");
        checkCondition(root, er.conditionOf(appName, "t2").intersect(), "=($0, 6)");

        final RelNode join = root;

        ExtractionResult ce = ConditionExtractor.columnEquivalenceFrom(root).extract();
        Map<Integer, BitSet> bitSets = ce.conditionOf(join).toColumnEquality();
        assertThat(bitSets.get(0), is(BitSets.of(0, 3)));
        assertThat(bitSets.get(3), is(BitSets.of(0, 3)));

        final Label rootLabel = er.getRootLabel();
        System.out.println(rootLabel.toString());
    }

    @Test
    public void testScanLeftJoinOnCondition9() {
        // Equivalent SQL:
        //   SELECT *
        //   FROM t1 LEFT JOIN t2 ON t1.id = t2.id AND t2.id = 6
        final RelBuilder builder = getRelBuilder();
        RelNode root = builder.scan("t1")
            .scan("t2")
            .join(JoinRelType.LEFT,
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field(2, 0, 0),
                    builder.field(2, 1, 0)),
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field(2, 1, 0),
                    builder.literal(6)))
            .build();
        final String expected = ""
            + "LogicalJoin(condition=[AND(=($0, $3), =($3, 6))], joinType=[LEFT])\n"
            + "  LogicalTableScan(table=[[" + appName + ", t1]])\n"
            + "  LogicalTableScan(table=[[" + appName + ", t2]])\n";
        assertThat(str(root), is(expected));

        final ExtractionResult er = ConditionExtractor.partitioningConditionFrom(root).extract();

        checkCondition(root, er.conditionOf(appName, "t1").intersect(), "true");
        checkCondition(root, er.conditionOf(appName, "t2").intersect(), "=($0, 6)");

        final RelNode join = root;

        ExtractionResult ce = ConditionExtractor.columnEquivalenceFrom(root).extract();
        Map<Integer, BitSet> bitSets = ce.conditionOf(join).toColumnEquality();
        assertThat(bitSets.get(0), is(BitSets.of(0, 3)));
        assertThat(bitSets.get(3), is(BitSets.of(0, 3)));

        final Label rootLabel = er.getRootLabel();
        System.out.println(rootLabel.toString());
    }

    @Test
    public void testScanLeftJoinOnCondition10() {
        // Equivalent SQL:
        //   SELECT *
        //   FROM t1 LEFT JOIN t2 ON t1.id = t1.name AND t1.name = t2.id AND t1.id = 6
        final RelBuilder builder = getRelBuilder();
        RelNode root = builder.scan("t1")
            .scan("t2")
            .join(JoinRelType.LEFT,
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field(2, 0, 0),
                    builder.field(2, 0, 1)),
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field(2, 0, 1),
                    builder.field(2, 1, 0)),
                builder.call(SqlStdOperatorTable.EQUALS,
                    builder.field(2, 0, 0),
                    builder.literal(6)))
            .build();
        final String expected = ""
            + "LogicalJoin(condition=[AND(=($0, $1), =($1, $3), =($0, 6))], joinType=[LEFT])\n"
            + "  LogicalTableScan(table=[[" + appName + ", t1]])\n"
            + "  LogicalTableScan(table=[[" + appName + ", t2]])\n";
        assertThat(str(root), is(expected));

        final ExtractionResult er = ConditionExtractor.partitioningConditionFrom(root).extract();

        checkCondition(root, er.conditionOf(appName, "t1").intersect(), "true");
        checkCondition(root, er.conditionOf(appName, "t2").intersect(), "=($0, 6)");

        final RelNode join = root;

        ExtractionResult ce = ConditionExtractor.columnEquivalenceFrom(root).extract();
        Map<Integer, BitSet> bitSets = ce.conditionOf(join).toColumnEquality();
        assertThat(bitSets.get(0), is(BitSets.of(0, 3)));
        assertThat(bitSets.get(1), is(BitSets.of(1, 3)));
        assertThat(bitSets.get(3), is(BitSets.of(0, 1, 3)));

        final Label rootLabel = er.getRootLabel();
        System.out.println(rootLabel.toString());
    }
}
