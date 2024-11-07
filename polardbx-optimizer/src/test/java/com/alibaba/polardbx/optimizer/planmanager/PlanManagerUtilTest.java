package com.alibaba.polardbx.optimizer.planmanager;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.jdbc.RawString;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.BaseRuleTest;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.parse.FastsqlParser;
import com.clearspring.analytics.util.Lists;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.polardbx.common.properties.ConnectionProperties.ENABLE_PARAM_TYPE_CHANGE;
import static com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil.getRexNodeTableMap;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.AND;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * @author fangwu
 */
public class PlanManagerUtilTest extends BaseRuleTest {

    @Test
    public void testRelNodeToJsonWithNullPlan() {
        assert null == PlanManagerUtil.relNodeToJson(null, false);
    }

    @Test
    public void testGettingTableSetFromAst() {
        String sql1 = "WITH tb AS (\n"
            + "  SELECT\n"
            + "    a.c1 x, b.c2 y\n"
            + "  FROM \n"
            + "    db1.t1 a\n"
            + "    JOIN db4.t2 b on a.name=b.name\n"
            + ")\n"
            + "\n"
            + "SELECT \n"
            + "  (tb.x + 1),\n"
            + "  concat(tb.y, 'xx', a.name),\n"
            + "  (select max(salary) from db5.t3),\n"
            + "  b.salary\n"
            + "FROM \n"
            + "  db2.t1 a \n"
            + "  JOIN db3.t2 b on a.id = b.id\n"
            + "  JOIN tb c on a.age = c.age\n"
            + "WHERE\n"
            + "\tb.val > (select avg(val) from db6.t1)\n"
            + "  AND NOT EXISTS ( SELECT 1 FROM t1 d WHERE d.id > 0);";
        SqlNode sqlNode1 = new FastsqlParser().parse(sql1).get(0);
        Set<Pair<String, String>> tableSet1 = PlanManagerUtil.getTableSetFromAst(sqlNode1);
        Set<Pair<String, String>> real1 = new HashSet<>();
        real1.add(Pair.of("db1", "t1"));
        real1.add(Pair.of("db4", "t2"));
        real1.add(Pair.of("db5", "t3"));
        real1.add(Pair.of("db2", "t1"));
        real1.add(Pair.of("db3", "t2"));
        real1.add(Pair.of("db6", "t1"));
        real1.add(Pair.of(null, "t1"));
        Assert.assertTrue(tableSet1.size() == real1.size());
        for (Pair<String, String> pair : tableSet1) {
            if (!real1.contains(pair)) {
                Assert.fail();
            }
        }

        String sql2 = "Select * from b.t b join a.t a on a.id=b.id where a.id = 2";
        SqlNode sqlNode2 = new FastsqlParser().parse(sql2).get(0);
        Set<Pair<String, String>> tableSet2 = PlanManagerUtil.getTableSetFromAst(sqlNode2);
        Set<Pair<String, String>> real2 = new HashSet<>();
        real2.add(Pair.of("a", "t"));
        real2.add(Pair.of("b", "t"));
        Assert.assertTrue(tableSet2.size() == real2.size());
        for (Pair<String, String> pair : tableSet2) {
            if (!real2.contains(pair)) {
                Assert.fail();
            }
        }

        String sql3 = "select * from (select * from a.t1 a join b.t2 b on a.id = b.id)";
        SqlNode sqlNode3 = new FastsqlParser().parse(sql3).get(0);
        Set<Pair<String, String>> tableSet3 = PlanManagerUtil.getTableSetFromAst(sqlNode3);
        Set<Pair<String, String>> real3 = new HashSet<>();
        real3.add(Pair.of("a", "t1"));
        real3.add(Pair.of("b", "t2"));
        Assert.assertTrue(tableSet3.size() == real3.size());
        for (Pair<String, String> pair : tableSet3) {
            if (!real3.contains(pair)) {
                Assert.fail();
            }
        }

    }

    /**
     * Does nothing if dynamic configuration disables parameter type changes.
     */
    @Test
    public void testChangeParameterTypeByTableMetadataWhenDisabled() {
        try {
            DynamicConfig.getInstance().loadValue(null, ENABLE_PARAM_TYPE_CHANGE, "true");

            ExecutionContext mockExecutionContext = mock(ExecutionContext.class);
            ExecutionPlan mockExecutionPlan = mock(ExecutionPlan.class);

            DynamicConfig.getInstance().setEnableChangeParamTypeByMeta(false);

            PlanManagerUtil.changeParameterTypeByTableMetadata(mockExecutionContext, mockExecutionPlan);

            // Verify no interactions occurred with execution context indicating no attempt at type conversion was made.
            Mockito.verifyZeroInteractions(mockExecutionContext);
        } finally {
            DynamicConfig.getInstance().loadValue(null, ENABLE_PARAM_TYPE_CHANGE, "false");
        }
    }

    /**
     * When retrieving the table expression map from the execution plan fails,
     * the function should return directly.
     * Expected behavior: Direct return when getting the table expression map returns null.
     */
    @Test
    public void testChangeParameterTypeByTableMetadataWithNullTableExpressionMap() {
        try {
            DynamicConfig.getInstance().loadValue(null, ENABLE_PARAM_TYPE_CHANGE, "true");

            ExecutionContext executionContext = mock(ExecutionContext.class);
            ExecutionPlan plan = mock(ExecutionPlan.class);
            when(getRexNodeTableMap(plan.getPlan())).thenReturn(null);

            PlanManagerUtil.changeParameterTypeByTableMetadata(executionContext, plan);

            verifyNoInteractions(executionContext);
        } finally {
            DynamicConfig.getInstance().loadValue(null, ENABLE_PARAM_TYPE_CHANGE, "false");
        }
    }

    /**
     * When the table expression map retrieved from the execution plan is empty,
     * the function should return directly.
     * Expected behavior: Direct return when the expression map is empty.
     */
    @Test
    public void testChangeParameterTypeByTableMetadataWithEmptyTableExpressionMap() {
        try {
            DynamicConfig.getInstance().loadValue(null, ENABLE_PARAM_TYPE_CHANGE, "true");

            ExecutionContext executionContext = mock(ExecutionContext.class);
            ExecutionPlan plan = mock(ExecutionPlan.class);

            PlanManagerUtil.changeParameterTypeByTableMetadata(executionContext, plan);

            verifyNoInteractions(executionContext);
        } finally {
            DynamicConfig.getInstance().loadValue(null, ENABLE_PARAM_TYPE_CHANGE, "false");
        }
    }

    @Test
    public void testChangeParameterTypeByTableMetadata() {
        try {
            DynamicConfig.getInstance().loadValue(null, ENABLE_PARAM_TYPE_CHANGE, "true");
            ExecutionContext executionContext = new ExecutionContext("optest");
            executionContext.setParams(new Parameters());
            executionContext.getParams().getCurrentParameter()
                .put(1, new ParameterContext(ParameterMethod.setString, new Object[] {1, "1"}));
            executionContext.getParams().getCurrentParameter()
                .put(2, new ParameterContext(ParameterMethod.setInt, new Object[] {2, 2}));
            executionContext.getParams().getCurrentParameter()
                .put(3, new ParameterContext(ParameterMethod.setObject1, new Object[] {
                    3, new RawString(
                    ImmutableList.of("1", "2", "3"))}));
            executionContext.getParams().getCurrentParameter()
                .put(4, new ParameterContext(ParameterMethod.setObject1, new Object[] {
                    4, new RawString(
                    ImmutableList.of(1, 2, 3))}));

            ExecutionPlan plan = mock(ExecutionPlan.class);
            LogicalTableScan scan = LogicalTableScan.create(relOptCluster,
                schema.getTableForMember(Arrays.asList("optest", "emp")));
            LogicalView logicalView = LogicalView.create(scan, scan.getTable());
            final RexBuilder rexBuilder = relOptCluster.getRexBuilder();
            JavaTypeFactory typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
            RelDataType rowTypeInt = typeFactory.createSqlType(SqlTypeName.BIGINT);
            RelDataType rowTypeChar = typeFactory.createSqlType(SqlTypeName.VARCHAR);

            // int = varchar
            RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                rexBuilder.makeFieldAccess(rexBuilder.makeRangeReference(logicalView), "userId", true),
                rexBuilder.makeDynamicParam(rowTypeChar, 0));

            condition = rexBuilder.makeCall(AND, condition, rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                rexBuilder.makeDynamicParam(rowTypeChar, 0),
                rexBuilder.makeFieldAccess(rexBuilder.makeRangeReference(logicalView), "userId", true)
            ));

            // varchar = int
            condition = rexBuilder.makeCall(AND, condition, rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                rexBuilder.makeDynamicParam(rowTypeInt, 1),
                rexBuilder.makeFieldAccess(rexBuilder.makeRangeReference(logicalView), "name", true)));

            condition = rexBuilder.makeCall(AND, condition, rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                rexBuilder.makeFieldAccess(rexBuilder.makeRangeReference(logicalView), "name", true),
                rexBuilder.makeDynamicParam(rowTypeInt, 1)));

            // int IN ROW(varchar)
            condition = rexBuilder.makeCall(AND, condition,
                rexBuilder.makeCall(SqlStdOperatorTable.IN,
                    rexBuilder.makeFieldAccess(rexBuilder.makeRangeReference(logicalView), "userId", true),
                    rexBuilder.makeCall(SqlStdOperatorTable.ROW, rexBuilder.makeDynamicParam(rowTypeChar, 2)))
            );

            // varchar IN ROW(int)
            condition = rexBuilder.makeCall(AND, condition,
                rexBuilder.makeCall(SqlStdOperatorTable.IN,
                    rexBuilder.makeFieldAccess(rexBuilder.makeRangeReference(logicalView), "name", true),
                    rexBuilder.makeCall(SqlStdOperatorTable.ROW, rexBuilder.makeDynamicParam(rowTypeInt, 3)))
            );

            LogicalFilter filter = LogicalFilter.create(logicalView, condition);
            when(plan.getPlan()).thenReturn(filter);
            System.out.println(executionContext.getParams());
            assertTrue("['1', 2, Raw('1','2','3'), Raw(1,2,3)]".equals(executionContext.getParams().toString()));
            PlanManagerUtil.changeParameterTypeByTableMetadata(executionContext, plan);

            System.out.println(executionContext.getParams());
            assertTrue("[1, '2', Raw(1,2,3), Raw('1','2','3')]".equals(executionContext.getParams().toString()));
        } finally {
            DynamicConfig.getInstance().loadValue(null, ENABLE_PARAM_TYPE_CHANGE, "false");
        }
    }

    /**
     * Test case 1: Verify that when an empty string is passed, the correct fixed-length hex string is returned.
     * Input: ""
     * Expected output: "00000000" (Assuming TStringUtil.int2FixedLenHexStr converts integers to an 8-character hex string)
     */
    @Test
    public void testGenerateTemplateIdWithEmptyString() {
        String input = "";
        String expectedOutput =
            "00000000"; // Assuming hashCode(input) == 0, and the resulting hex string is also 8 characters long
        String actualOutput = PlanManagerUtil.generateTemplateId(input);

        assertEquals(expectedOutput, actualOutput);
    }

    /**
     * Test case 2: Confirm that when a specific string is passed, the correct fixed-length hex string is returned.
     * Input: "SELECT * FROM table WHERE id = ?"
     * Output: Fixed-length hex string calculated from the hashCode of the above string
     */
    @Test
    public void testGenerateTemplateIdWithSpecificString() {
        String input = "SELECT * FROM table WHERE id = ?";
        String expectedOutput = Integer.toHexString(input.hashCode()).toLowerCase(); // Calculate expected output
        while (expectedOutput.length()
            < 8) { // Ensure the string length is at least 8 characters long (pad with zeros if necessary)
            expectedOutput = "0" + expectedOutput;
        }
        String actualOutput = PlanManagerUtil.generateTemplateId(input);

        assertEquals(expectedOutput.toLowerCase(),
            actualOutput); // Compare outputs after converting both strings to uppercase for consistent comparison
    }

    /**
     * Test case 3: Check behavior with a longer string that results in a larger hashCode value.
     * Input: "A very long SQL query with multiple conditions and joins."
     * Expected output: Fixed-length hex string derived from the hashCode of the input string
     */
    @Test
    public void testGenerateTemplateIdWithLongerString() {
        String input = "A very long SQL query with multiple conditions and joins.";
        String expectedOutput = Integer.toHexString(input.hashCode()).toLowerCase();
        while (expectedOutput.length() < 8) {
            expectedOutput = "0" + expectedOutput; // Pad with leading zeros to maintain fixed length
        }
        String actualOutput = PlanManagerUtil.generateTemplateId(input);

        assertEquals(expectedOutput.toLowerCase(), actualOutput); // Perform comparison in uppercase for consistency
    }

    @Test
    public void testConvertSimpleStringToLong() {
        ExecutionContext mockExecutionContext;
        Parameters mockParams;
        ParameterContext mockParameterContext;
        RawString mockRawString;
        mockExecutionContext = Mockito.mock(ExecutionContext.class);
        mockParams = Mockito.mock(Parameters.class);
        mockParameterContext = Mockito.mock(ParameterContext.class);
        mockRawString = Mockito.mock(RawString.class);

        // 设置mock对象行为
        when(mockExecutionContext.getParams()).thenReturn(mockParams);
        when(mockParams.getCurrentParameter()).thenReturn(Maps.newHashMap());
        List<Object> objList = Lists.newArrayList();
        objList.add("10");
        objList.add("20");
        when(mockParameterContext.getValue()).thenReturn("10", "test", new RawString(objList));

        Set<Integer> indices = Sets.newHashSet();
        indices.add(0);
        Map<Integer, ParameterContext> map = Maps.newHashMap();
        map.put(1, mockParameterContext);
        when(mockParams.getCurrentParameter()).thenReturn(map);

        PlanManagerUtil.convertCharToIntParameters(indices, mockExecutionContext);

        verify(mockParameterContext).setParameterMethod(ParameterMethod.setLong);
        verify(mockParameterContext).setValue(10L);
    }

    @Test
    public void testConvertRawStringToListOfLongs() {
        ExecutionContext mockExecutionContext;
        Parameters mockParams;

        ParameterContext mockParameterContext = new ParameterContext(ParameterMethod.setObject1, new Object[] {1, 20});
        mockExecutionContext = Mockito.mock(ExecutionContext.class);
        mockParams = Mockito.mock(Parameters.class);

        Set<Integer> indices = Sets.newHashSet();
        indices.add(1);
        Map<Integer, ParameterContext> map = Maps.newHashMap();
        map.put(2, mockParameterContext);
        when(mockExecutionContext.getParams()).thenReturn(mockParams);
        when(mockParams.getCurrentParameter()).thenReturn(map);
        List<Object> objList = Lists.newArrayList();
        objList.add("10");
        objList.add("20");
        List<Object> objListCheck = Lists.newArrayList();
        objListCheck.add(10L);
        objListCheck.add(20L);
        RawString mockRawString = new RawString(objList);
        mockParameterContext.setValue(mockRawString);

        PlanManagerUtil.convertCharToIntParameters(indices, mockExecutionContext);

        RawString result = (RawString) mockParameterContext.getValue();
        assertEquals(objListCheck, result.getObjList());
    }

    @Test
    public void testConvertIntToCharParametersWithEmptyList() {
        Set<Integer> intToCharIndices = Sets.newHashSet();
        ExecutionContext executionContext = new ExecutionContext();
        executionContext.setParams(new Parameters());
        executionContext.getParams().getCurrentParameter()
            .put(1, new ParameterContext(ParameterMethod.setObject1, new Object[] {1, 20}));

        PlanManagerUtil.convertIntToCharParameters(intToCharIndices, executionContext);

        assertTrue(executionContext.getParams().getCurrentParameter().get(1).getValue() instanceof Integer);
    }

    @Test
    public void testConvertIntToCharParametersWithNumberValue() {
        Set<Integer> intToCharIndices = Sets.newHashSet();
        intToCharIndices.add(0); // 假设第一个参数应该被转换
        ExecutionContext executionContext = new ExecutionContext();
        executionContext.setParams(new Parameters());
        executionContext.getParams().getCurrentParameter()
            .put(1, new ParameterContext(ParameterMethod.setObject1, new Object[] {1, 20}));

        PlanManagerUtil.convertIntToCharParameters(intToCharIndices, executionContext);

        assertTrue(executionContext.getParams().getCurrentParameter().get(1).getParameterMethod()
            == ParameterMethod.setString);
        assertTrue(executionContext.getParams().getCurrentParameter().get(1).getValue().equals("20")); // 验证参数方法被正确设置
    }

    @Test
    public void testConvertIntToCharParametersWithRawStringValue() {
        Set<Integer> intToCharIndices = Sets.newHashSet();
        intToCharIndices.add(1);

        List<Object> originalList = new ArrayList<>();
        originalList.add(1);
        originalList.add(2);
        originalList.add(3);
        RawString rawString = new RawString(originalList);
        ExecutionContext executionContext = new ExecutionContext();
        executionContext.setParams(new Parameters());
        executionContext.getParams().getCurrentParameter()
            .put(2, new ParameterContext(ParameterMethod.setObject1, new Object[] {2, rawString}));
        List<Object> objList = Lists.newArrayList();
        objList.add("1");
        objList.add("2");
        objList.add("3");
        PlanManagerUtil.convertIntToCharParameters(intToCharIndices, executionContext);
        RawString result = (RawString) executionContext.getParams().getCurrentParameter().get(2).getValue();

        assertTrue(executionContext.getParams().getCurrentParameter().get(2).getParameterMethod()
            == ParameterMethod.setObject1);
        assertEquals(objList, result.getObjList());
    }
}
