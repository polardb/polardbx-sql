package com.alibaba.polardbx.executor.vectorized.metadata;

import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.apache.calcite.rel.type.DynamicRecordTypeImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;

public class Rex2ArgTest {

    @Test
    public void testIoArgumentInfo() {
        ArgumentInfo argumentInfo = Rex2ArgumentInfo.toArgumentInfo(null);
        Assert.assertNull(argumentInfo);

        final RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexCall exprCall = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.MINUS,
            rexBuilder.makeDynamicParam(intType, 0),
            rexBuilder.makeDynamicParam(intType, 1));
        argumentInfo = Rex2ArgumentInfo.toArgumentInfo(exprCall);
        Assert.assertNotNull(argumentInfo);
        Assert.assertTrue(argumentInfo.getType().equalDeeply(DataTypes.LongType));
        Assert.assertSame(argumentInfo.getKind(), ArgumentKind.Variable);

        RexDynamicParam dynamicParam = rexBuilder.makeDynamicParam(intType, 0);
        argumentInfo = Rex2ArgumentInfo.toArgumentInfo(dynamicParam);
        Assert.assertNotNull(argumentInfo);
        Assert.assertSame(argumentInfo.getKind(), ArgumentKind.Const);

        exprCall = Mockito.mock(RexCall.class);
        Mockito.when(exprCall.accept(any(RexVisitor.class))).thenCallRealMethod();
        Mockito.when(exprCall.getType()).thenReturn(new DynamicRecordTypeImpl(typeFactory));
        argumentInfo = Rex2ArgumentInfo.toArgumentInfo(exprCall);
        Assert.assertNull(argumentInfo);

        DynamicRecordTypeImpl mockType = Mockito.mock(DynamicRecordTypeImpl.class);
        RelDataTypeFieldImpl field = new RelDataTypeFieldImpl("f1", 0, typeFactory.createSqlType(SqlTypeName.INTEGER));
        Mockito.when(mockType.getFieldList()).thenReturn(Arrays.asList(field));
        Mockito.when(mockType.isStruct()).thenReturn(true);
        Mockito.when(exprCall.getType()).thenReturn(mockType);
        argumentInfo = Rex2ArgumentInfo.toArgumentInfo(exprCall);
        Assert.assertNotNull(argumentInfo);
        Assert.assertSame(argumentInfo.getKind(), ArgumentKind.ConstVargs);
    }

    private List<RexNode> convertInValues(RelDataTypeFactory typeFactory, int[] inValues) {
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        List<RexNode> rexNodeList = new ArrayList<>(inValues.length + 1);
        rexNodeList.add(new RexInputRef(1, typeFactory.createSqlType(SqlTypeName.BIGINT)));
        for (long inValue : inValues) {
            RexNode rexNode = rexBuilder.makeLiteral(inValue,
                typeFactory.createSqlType(SqlTypeName.INTEGER), false);
            rexNodeList.add(rexNode);
        }
        return rexNodeList;
    }
}
