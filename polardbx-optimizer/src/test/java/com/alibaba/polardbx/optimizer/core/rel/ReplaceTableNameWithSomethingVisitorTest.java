package com.alibaba.polardbx.optimizer.core.rel;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;

import java.util.Map;

import static com.alibaba.polardbx.common.utils.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author fangwu
 */

public class ReplaceTableNameWithSomethingVisitorTest {

    /**
     * 测试用例1 - 当处于explain模式时且动态参数值为字段访问类型
     * 设计思路：模拟一个RexFieldAccess类型的动态参数，在解释模式下返回其符号表示形式。
     */
    @Test
    public void testBuildDynamicParamForCorrelatedSubqueryInExplainModeWithFieldAccess() {
        // 准备
        boolean isExplain = true;
        RexFieldAccess fieldAccess = mock(RexFieldAccess.class);
        when(fieldAccess.toString()).thenReturn("some_field");
        SqlDynamicParam dynamicParam = new SqlDynamicParam(0, SqlParserPos.ZERO, fieldAccess);

        // 执行
        SqlNode result =
            ReplaceTableNameWithSomethingVisitor.buildDynamicParamForCorrelatedSubquery(isExplain, dynamicParam,
                null);

        // 验证
        assert (result instanceof SqlLiteral);
        assertEquals(SqlLiteral.createSymbol("some_field", SqlParserPos.ZERO), result);
    }

    /**
     * 测试用例2 - 当处于explain模式时但动态参数不是字段访问类型
     * 设计思路：模拟一个非RexFieldAccess类型的动态参数，在解释模式下直接返回动态参数对象。
     */
    @Test
    public void testBuildDynamicParamForCorrelatedSubqueryInExplainModeWithoutFieldAccess() {
        // 准备
        boolean isExplain = true;
        RexNode fieldValue = mock(RexNode.class);
        SqlDynamicParam dynamicParam = new SqlDynamicParam(0, SqlParserPos.ZERO, fieldValue);

        // 执行
        SqlNode result =
            ReplaceTableNameWithSomethingVisitor.buildDynamicParamForCorrelatedSubquery(isExplain, dynamicParam,
                null);

        // 验证
        assert (result instanceof SqlDynamicParam);
        assertEquals(dynamicParam, result);
    }

    /**
     * 测试用例3 - 当不在explain模式时且相关字段映射中有对应的值
     * 设计思路：模拟一个非解释模式下的情况，其中相关字段映射包含对应值，检查是否正确构建SQL字面量节点。
     */
    @Test
    public void testBuildDynamicParamForCorrelatedSubqueryNotInExplainModeWithCorrelation() {
        // 准备
        boolean isExplain = false;
        RexLiteral fieldValue = mock(RexLiteral.class);
        when(fieldValue.getTypeName()).thenReturn(SqlTypeName.VARCHAR);
        when(fieldValue.getValue2()).thenReturn("test literal");
        SqlDynamicParam dynamicParam = new SqlDynamicParam(-4, SqlParserPos.ZERO, fieldValue);
        RexFieldAccess access = mock(RexFieldAccess.class);
        Map<RexFieldAccess, RexNode> correlateFieldMap = ImmutableMap.of(access, fieldValue);
        dynamicParam.setValue(access);

        // 执行
        SqlNode result =
            ReplaceTableNameWithSomethingVisitor.buildDynamicParamForCorrelatedSubquery(isExplain, dynamicParam,
                correlateFieldMap);

        // 验证
        assert (result instanceof SqlLiteral);
        assert result.toString().equalsIgnoreCase("'test literal'");
    }

    /**
     * 测试用例4 - 当不在explain模式时但在相关字段映射中找不到对应值
     * 设计思路：模拟一个非explain模式下的情况，其中相关字段映射不包含对应值，预期抛出IllegalArgumentException异常。
     */
    @Test
    public void testBuildDynamicParamForCorrelatedSubqueryNotInExplainModeWithoutCorrelation() {
        // 准备
        boolean isExplain = false;
        RexLiteral fieldValue = mock(RexLiteral.class);
        SqlDynamicParam dynamicParam = new SqlDynamicParam(0, SqlParserPos.ZERO, fieldValue);
        Map<RexFieldAccess, RexNode> correlateFieldMap = ImmutableMap.of();

        // 执行并验证
        try {
            ReplaceTableNameWithSomethingVisitor.buildDynamicParamForCorrelatedSubquery(isExplain, dynamicParam,
                correlateFieldMap);
            fail("Expected an IllegalArgumentException to be thrown.");
        } catch (IllegalArgumentException e) {
            assertEquals("Corresponding item not found in correlated field map: " + fieldValue, e.getMessage());
        }
    }
}

