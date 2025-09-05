package com.alibaba.polardbx.executor.vectorized.build;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

// test partial folding.
public class FoldingCaseTest {
    private final static RelDataTypeFactory TYPE_FACTORY =
        new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
    private final static RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);

    public void doTestPartialFolding(SqlOperator operator) {

        //  the operands tree:
        //
        //       DATE_ADD
        //       /      \
        //   TIME   INTERVAL_PRIMARY
        //                 /    \
        //           VALUE        TIME_UNIT

        List<RexNode> operands = new ArrayList<>();

        RexCall dateValue = (RexCall) REX_BUILDER.makeCall(TddlOperatorTable.NOW, ImmutableList.of());
        operands.add(dateValue);

        RexCall intervalValue =
            (RexCall) REX_BUILDER.makeCall(TddlOperatorTable.INTERVAL_PRIMARY,
                ImmutableList.of(
                    REX_BUILDER.makeIntLiteral(10),
                    REX_BUILDER.makeLiteral("DAYS")
                )
            );

        // Create a RexCall with the specified operator and operands
        RexCall call = (RexCall) REX_BUILDER.makeCall(
            TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT),
            operator,
            ImmutableList.of(dateValue, intervalValue)
        );

        ExpressionRewriter.RexConstantFoldShuttle shuttle = new ExpressionRewriter.RexConstantFoldShuttle(
            REX_BUILDER, new ExecutionContext(), null, null
        );

        RexNode node = call.accept(shuttle);
        Assert.assertTrue(node == call);
    }

    @Test
    public void test() {
        doTestPartialFolding(TddlOperatorTable.DATE_ADD);
        doTestPartialFolding(TddlOperatorTable.DATE_SUB);
        doTestPartialFolding(TddlOperatorTable.ADDDATE);
        doTestPartialFolding(TddlOperatorTable.SUBDATE);
    }
}
