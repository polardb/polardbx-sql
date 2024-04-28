package com.alibaba.polardbx.optimizer.view;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.RawString;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.planner.common.BasePlannerTest;
import com.alibaba.polardbx.planner.common.EclipseParameterized;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@RunWith(EclipseParameterized.class)
public class VirtualViewTest extends BasePlannerTest {

    private static final Set<String> RESULT_SET = Collections.singleton("correct");
    private static final Set<String> WRONG_SET = Sets.newHashSet("wrong_1", "wrong_2", "wrong_3");
    private static final Set<String> UNIVERSAL_SET = Sets.union(RESULT_SET, WRONG_SET);
    private static final String SCHEMA = "INFORMATION_SCHEMA";

    final private VirtualViewType virtualViewType;
    private VirtualView view;
    private List<Integer> indexableColumnList;
    private RexBuilder rexBuilder;
    private RelDataTypeFactory typeFactory;

    public VirtualViewTest(VirtualViewType virtualViewType) {
        super(SCHEMA);
        this.virtualViewType = virtualViewType;
    }

    @Parameterized.Parameters(name = "{0}")
    public static List<Object[]> prepare() {
        return Arrays.stream(VirtualViewType.values()).map(type -> new Object[] {
            type,
        }).collect(Collectors.toList());
    }

    @Before
    public void prepareView() {
        RelOptCluster cluster = SqlConverter.getInstance(SCHEMA, ec).createRelOptCluster(new PlannerContext(ec));
        view = VirtualView.create(cluster, virtualViewType);

        indexableColumnList = view.indexableColumnList();

        rexBuilder = cluster.getRexBuilder();
        typeFactory = cluster.getTypeFactory();
    }

    @Test
    public void testPushFilterEquals() {
        for (int index : indexableColumnList) {
            RexNode rexCall = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), index),
                rexBuilder.makeDynamicParam(typeFactory.createSqlType(SqlTypeName.VARCHAR), index));
            view.pushFilter(rexCall);
        }

        for (int index : indexableColumnList) {
            Map<Integer, ParameterContext> paramMap = new HashMap<>();
            paramMap.put(index + 1,
                new ParameterContext(ParameterMethod.setObject1, new Object[] {index + 1, "correct"}));

            Assert.assertEquals(RESULT_SET, view.getEqualsFilterValues(index, paramMap));
            Assert.assertEquals(RESULT_SET, view.applyFilters(index, paramMap, UNIVERSAL_SET));
            Assert.assertEquals(Collections.EMPTY_SET, view.applyFilters(index, paramMap, WRONG_SET));
        }
    }

    @Test
    public void testPushFilterEqualsLiteral() {
        for (int index : indexableColumnList) {
            RexNode rexCall = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), index),
                rexBuilder.makeLiteral("correct"));
            view.pushFilter(rexCall);
        }

        for (int index : indexableColumnList) {
            Map<Integer, ParameterContext> paramMap = new HashMap<>();

            Assert.assertEquals(RESULT_SET, view.getEqualsFilterValues(index, paramMap));
            Assert.assertEquals(RESULT_SET, view.applyFilters(index, paramMap, UNIVERSAL_SET));
            Assert.assertEquals(Collections.EMPTY_SET, view.applyFilters(index, paramMap, WRONG_SET));
        }
    }

    @Test
    public void testPushFilterIn() {
        for (int index : indexableColumnList) {
            RexNode rexCall = rexBuilder.makeCall(SqlStdOperatorTable.IN,
                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), index),
                rexBuilder.makeCall(SqlStdOperatorTable.ROW,
                    rexBuilder.makeDynamicParam(typeFactory.createSqlType(SqlTypeName.VARCHAR), index)));
            view.pushFilter(rexCall);
        }

        for (int index : indexableColumnList) {
            Map<Integer, ParameterContext> paramMap = new HashMap<>();
            paramMap.put(index + 1,
                new ParameterContext(ParameterMethod.setObject1, new Object[] {
                    index + 1, new RawString(
                    Lists.newArrayList("correct"))}));

            Assert.assertEquals(RESULT_SET, view.applyFilters(index, paramMap, UNIVERSAL_SET));
            Assert.assertEquals(Collections.EMPTY_SET, view.applyFilters(index, paramMap, WRONG_SET));
        }
    }

    @Test
    public void testPushFilterInLiteral() {
        for (int index : indexableColumnList) {
            RexNode rexCall = rexBuilder.makeCall(SqlStdOperatorTable.IN,
                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), index),
                rexBuilder.makeCall(SqlStdOperatorTable.ROW, rexBuilder.makeLiteral("correct")));
            view.pushFilter(rexCall);
        }

        for (int index : indexableColumnList) {
            Map<Integer, ParameterContext> paramMap = new HashMap<>();

            Assert.assertEquals(RESULT_SET, view.applyFilters(index, paramMap, UNIVERSAL_SET));
            Assert.assertEquals(Collections.EMPTY_SET, view.applyFilters(index, paramMap, WRONG_SET));
        }
    }

    @Test
    public void testPushFilterLike() {
        for (int index : indexableColumnList) {
            RexNode rexCall = rexBuilder.makeCall(SqlStdOperatorTable.LIKE,
                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), index),
                rexBuilder.makeDynamicParam(typeFactory.createSqlType(SqlTypeName.VARCHAR), index));
            view.pushFilter(rexCall);
        }

        for (int index : indexableColumnList) {
            Map<Integer, ParameterContext> paramMap = new HashMap<>();
            paramMap.put(index + 1,
                new ParameterContext(ParameterMethod.setObject1, new Object[] {index + 1, "%orrec%"}));

            Assert.assertEquals("%orrec%", view.getLikeString(index, paramMap));
            Assert.assertEquals(RESULT_SET, view.applyFilters(index, paramMap, UNIVERSAL_SET));
            Assert.assertEquals(Collections.EMPTY_SET, view.applyFilters(index, paramMap, WRONG_SET));
        }
    }

    @Test
    public void testPushFilterLiteral() {
        for (int index : indexableColumnList) {
            RexNode rexCall = rexBuilder.makeCall(SqlStdOperatorTable.LIKE,
                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.VARCHAR), index),
                rexBuilder.makeLiteral("%orrec%"));
            view.pushFilter(rexCall);
        }

        for (int index : indexableColumnList) {
            Map<Integer, ParameterContext> paramMap = new HashMap<>();

            Assert.assertEquals("%orrec%", view.getLikeString(index, paramMap));
            Assert.assertEquals(RESULT_SET, view.applyFilters(index, paramMap, UNIVERSAL_SET));
            Assert.assertEquals(Collections.EMPTY_SET, view.applyFilters(index, paramMap, WRONG_SET));
        }
    }

    @Override
    protected String getPlan(String testSql) {
        return null;
    }
}
