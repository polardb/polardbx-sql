package com.alibaba.polardbx.executor.vectorized;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.executor.vectorized.build.InputRefTypeChecker;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

public class InputRefTypeCheckerTest {
    private final static RelDataTypeFactory TYPE_FACTORY =
        new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());

    private final static RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);

    @Test
    public void testDiffType() {
        // input type of exec: bigint, integer
        List<DataType<?>> inputTypes = ImmutableList.of(
            DataTypes.LongType,
            DataTypes.IntegerType
        );

        // rex call tree:
        // MULTIPLY
        //   /   \
        // int  bigint
        RexInputRef inputRef1 = REX_BUILDER.makeInputRef(
            TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER),
            0
        );
        RexInputRef inputRef2 = REX_BUILDER.makeInputRef(
            TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT),
            1
        );
        RexCall call = (RexCall) REX_BUILDER.makeCall(
            TddlOperatorTable.MULTIPLY,
            inputRef1,
            inputRef2
        );

        // force the operand type to input type.
        InputRefTypeChecker checker = new InputRefTypeChecker(inputTypes);
        RexCall newCall = (RexCall) call.accept(checker);

        // collect new input type and check.
        List<DataType> newInputTypes = newCall.getOperands().stream().map(
            operand -> DataTypeUtil.calciteToDrdsType(operand.getType())
        ).collect(Collectors.toList());

        for (int i = 0; i < inputTypes.size(); i++) {
            Assert.assertTrue(DataTypeUtil.equalsSemantically(
                inputTypes.get(i),
                newInputTypes.get(i)
            ));
        }
    }
}
