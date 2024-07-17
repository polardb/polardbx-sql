package com.alibaba.polardbx.executor.vectorized.math;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLInterval;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLIntervalType;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLTimeCalculator;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalDate;
import com.alibaba.polardbx.executor.chunk.BlockUtils;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.DateBlock;
import com.alibaba.polardbx.executor.chunk.DateBlockBuilder;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.operator.BaseExecTest;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.build.InputRefTypeChecker;
import com.alibaba.polardbx.executor.vectorized.build.Rex2VectorizedExpressionVisitor;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class ExtractVectorizedExpressionTest extends BaseExecTest {
    private final static RelDataTypeFactory TYPE_FACTORY =
        new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
    private final static RexBuilder REX_BUILDER = new RexBuilder(TYPE_FACTORY);

    private final static Random RANDOM = new Random();

    private final static MysqlDateTime START_DATE = new MysqlDateTime(1970, 1, 1, 0, 0, 0, 0);
    private final static MysqlDateTime END_DATE = new MysqlDateTime(2020, 1, 1, 0, 0, 0, 0);

    @Before
    public void before() {
        Map connectionMap = new HashMap();
        connectionMap.put(ConnectionParams.CHUNK_SIZE.getName(), 1000);
        connectionMap.put(ConnectionParams.ENABLE_EXPRESSION_VECTORIZATION.getName(), true);
        context.setParamManager(new ParamManager(connectionMap));
    }

    @Test
    public void test() {
        // extract(year from col_date)

        final int positionCount = context.getExecutorChunkLimit();
        final int nullCount = 20;
        final int lowerBound = 0; // 1970-01-01
        final int upperBound = 365 * 50; // days
        final SqlOperator operator = TddlOperatorTable.EXTRACT;

        List<DataType<?>> inputTypes = ImmutableList.of(DataTypes.DateType);

        RexNode root = REX_BUILDER.makeCall(
            TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT),
            operator,
            ImmutableList.of(
                REX_BUILDER.makeLiteral("YEAR"),
                REX_BUILDER.makeInputRef(TYPE_FACTORY.createSqlType(SqlTypeName.DATE), 0)
            ));

        InputRefTypeChecker inputRefTypeChecker = new InputRefTypeChecker(inputTypes);
        root = root.accept(inputRefTypeChecker);

        Rex2VectorizedExpressionVisitor converter =
            new Rex2VectorizedExpressionVisitor(context, inputTypes.size());

        VectorizedExpression expression = root.accept(converter);

        MutableChunk preAllocatedChunk = MutableChunk.newBuilder(positionCount)
            .addEmptySlots(inputTypes)
            .addEmptySlots(converter.getOutputDataTypes())
            .build();

        // build input decimal block
        DateBlock inputBlock =
            generateDateBlock(positionCount, nullCount, lowerBound, upperBound);
        Chunk inputChunk = new Chunk(positionCount, inputBlock);

        LongBlock outputBlock = (LongBlock) BlockUtils.createBlock(DataTypes.LongType, inputChunk.getPositionCount());

        preAllocatedChunk.setSelection(null);
        preAllocatedChunk.setSelectionInUse(false);
        preAllocatedChunk.setSlotAt(inputBlock, 0);
        preAllocatedChunk.setSlotAt(outputBlock, expression.getOutputIndex());
        preAllocatedChunk.setBatchSize(positionCount);

        EvaluationContext evaluationContext = new EvaluationContext(preAllocatedChunk, context);
        expression.eval(evaluationContext);

        for (int i = 0; i < inputChunk.getPositionCount(); i++) {
            Object actual = outputBlock.getObject(i);
            Object expected = inputBlock.isNull(i) ? null :
                ((OriginalDate) inputBlock.getDate(i)).getMysqlDateTime().getYear();

            Assert.assertEquals(expected, actual);
        }
    }

    private DateBlock generateDateBlock(int positionCount, int nullCount,
                                        int lowerBound, int upperBound) {
        DateBlockBuilder blockBuilder = new DateBlockBuilder(positionCount, DataTypes.DateType, context);
        for (int i = 0; i < positionCount; i++) {
            if (RANDOM.nextInt(positionCount) < nullCount) {
                blockBuilder.appendNull();
            } else {
                int days = RANDOM.nextInt(upperBound - lowerBound) + lowerBound;
                MySQLInterval interval = new MySQLInterval();
                interval.setDay(days);
                MysqlDateTime result =
                    MySQLTimeCalculator.addInterval(START_DATE, MySQLIntervalType.INTERVAL_DAY, interval);

                blockBuilder.writeMysqlDatetime(result);
            }
        }
        return (DateBlock) blockBuilder.build();
    }

}
