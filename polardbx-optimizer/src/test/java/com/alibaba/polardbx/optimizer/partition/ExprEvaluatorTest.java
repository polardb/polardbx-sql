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

package com.alibaba.polardbx.optimizer.partition;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.partition.pruning.ExprEvaluator;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.google.common.primitives.Longs;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.optimizer.utils.ExprContextProvider;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlPartitionValueItem;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author chenghui.lch
 */
public class ExprEvaluatorTest {

    @Test
    public void testLongToArr() {

        long s = -2;
        byte[] byteArr = Longs.toByteArray(s);
        for (int i = 0; i < byteArr.length; i++) {
            if (i > 0) {
                System.out.print(",");
            }
            System.out.print(Integer.toHexString(byteArr[i] & 0xFF));
        }
        System.out.print("\n");
    }

    @Test
    public void testArray() {

        TestA[][] testArr = new TestA[2][3];
        for (int i = 0; i < 2; i++) {
            for (int j = 0; j < 3; j++) {
                TestA testA = testArr[i][j];
                if (testA == null) {
                    System.out.println(String.format("test is null: %s,%s",i,j));
                }
            }
        }
        System.out.print(testArr);
        System.out.println("test array " );
    }

    protected static class TestA {
        static int cnt = 0;
        int a;
        public TestA()  {
            ++cnt;
            System.out.println("a " + cnt);
        }
    }

    @Test
    public void testEvalDynamicExpr() {
        try {

            final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
            RexBuilder rexBuilder = new RexBuilder(typeFactory);

            RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
            RexCall exprCall = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.PLUS,
                rexBuilder.makeDynamicParam(intType, 0),
                rexBuilder.makeDynamicParam(intType, 1));

            ExprContextProvider holder = new ExprContextProvider();
            IExpression exprCalc = RexUtils.getEvalFuncExec(exprCall, holder);
            ExprEvaluator ee = new ExprEvaluator(holder, exprCalc);

            ExecutionContext context1 = new ExecutionContext();
            Map<Integer, ParameterContext> paramMap1 = new HashMap<>();
            paramMap1.put(1,new ParameterContext(ParameterMethod.setObject1, new Object[]{1, 5}));
            paramMap1.put(2,new ParameterContext(ParameterMethod.setObject1, new Object[]{2, 5}));
            context1.setParams(new Parameters(paramMap1));
            Object rs = ee.eval(context1);
            System.out.println(rs);
            Assert.assertTrue("10".equalsIgnoreCase(String.valueOf(rs)));

            ExecutionContext context2 = new ExecutionContext();
            Map<Integer, ParameterContext> paramMap2 = new HashMap<>();
            paramMap2.put(1,new ParameterContext(ParameterMethod.setObject1, new Object[]{1, 7}));
            paramMap2.put(2,new ParameterContext(ParameterMethod.setObject1, new Object[]{2, 9}));
            context2.setParams(new Parameters(paramMap2));
            Object rs2 = ee.eval(context2);
            System.out.println(rs2);
            Assert.assertTrue("16".equalsIgnoreCase(String.valueOf(rs2)));

            ExecutionContext context3 = new ExecutionContext();
            Map<Integer, ParameterContext> paramMap3 = new HashMap<>();
            paramMap3.put(1,new ParameterContext(ParameterMethod.setObject1, new Object[]{1, 8}));
            paramMap3.put(2,new ParameterContext(ParameterMethod.setObject1, new Object[]{2, 10}));
            context3.setParams(new Parameters(paramMap3));
            Object rs3 = ee.eval(context3);
            System.out.println(rs3);
            Assert.assertTrue("18".equalsIgnoreCase(String.valueOf(rs3)));

        } catch (Throwable ex) {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        }

    }

    @Test
    public void testSqlToRex() {
        try {

            final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
            RexBuilder rexBuilder = new RexBuilder(typeFactory);

            RexCall exprCall = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.PLUS,
                rexBuilder.makeIntLiteral(10),
                rexBuilder.makeIntLiteral(1));

            ExprContextProvider holder = new ExprContextProvider();
            IExpression exprCalc = RexUtils.getEvalFuncExec(exprCall, holder);
            ExprEvaluator ee = new ExprEvaluator(holder, exprCalc);

            ExecutionContext context1 = new ExecutionContext();
            Object rs = ee.eval(context1);
            System.out.println(rs);

            ExecutionContext context2 = new ExecutionContext();
            Object rs2 = ee.eval(context2);
            System.out.println(rs2);

            ExecutionContext context3 = new ExecutionContext();
            Object rs3 = ee.eval(context3);
            System.out.println(rs3);

        } catch (Throwable ex) {
            ex.printStackTrace();
        }

    }


    @Test
    public void testEvalSimpleExpr() {
        try {

            final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
            RexBuilder rexBuilder = new RexBuilder(typeFactory);

            List<SqlPartitionValueItem> partFnList =
                PartitionInfoUtil.buildPartitionExprByString("pk,year(gmt_modified)");

            //SqlNodeToRexConverter sql2Rex = new SqlNodeToRexConverterImpl(StandardConvertletTable.INSTANCE);
            RexCall exprCall = null;

            ExprContextProvider holder = new ExprContextProvider();
            IExpression exprCalc = RexUtils.getEvalFuncExec(exprCall, holder);
            ExprEvaluator ee = new ExprEvaluator(holder, exprCalc);

            ExecutionContext context1 = new ExecutionContext();
            Object rs = ee.eval(context1);
            System.out.println(rs);

            ExecutionContext context2 = new ExecutionContext();
            Object rs2 = ee.eval(context2);
            System.out.println(rs2);

            ExecutionContext context3 = new ExecutionContext();
            Object rs3 = ee.eval(context3);
            System.out.println(rs3);

        } catch (Throwable ex) {
            ex.printStackTrace();
        }

    }
}
