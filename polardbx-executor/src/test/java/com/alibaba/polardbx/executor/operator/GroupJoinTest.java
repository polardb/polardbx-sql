package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.StringBlock;
import com.alibaba.polardbx.executor.operator.util.EquiJoinMockData;
import com.alibaba.polardbx.executor.operator.util.RowChunksBuilder;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.CountV2;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinKey;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.core.JoinRelType;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.executor.operator.util.RowChunksBuilder.rowChunksBuilder;

public class GroupJoinTest extends HashJoinTest {
    @Before
    public void before() {
        Map connectionMap = new HashMap();
        connectionMap.put(ConnectionParams.CHUNK_SIZE.getName(), 1000);

        // open vectorization implementation o f join probing and rows building.
        connectionMap.put(ConnectionParams.ENABLE_VEC_JOIN.getName(), true);
        connectionMap.put(ConnectionParams.ENABLE_VEC_BUILD_JOIN_ROW.getName(), false);
        context.setParamManager(new ParamManager(connectionMap));
    }

    @Test
    @SuppressWarnings("SpellCheckingInspection") // for tpch column names
    public void testQ13() {
        //  subplan of Q13:
        //  select c_custkey, count(o_orderkey) as c_count
        //  from customer
        //  left outer join orders on c_custkey = o_custkey
        //  group by c_custkey

        List<DataType> outerTypes = ImmutableList.of(DataTypes.IntegerType);
        List<DataType> innerTypes = ImmutableList.of(DataTypes.LongType, DataTypes.IntegerType);

        // outer exec and types.
        // types = ImmutableList.of(DataTypes.IntegerType)
        // for exec returning c_custkey.
        final int lowerBound = 1;
        final int upperBound = 100;
        Executor outerInput = new MockExec(outerTypes, ImmutableList.of(
            new Chunk(IntegerBlock.of(new Integer[] {
                68, 69, 81, null, 35, 90, 38, 77, 76, null, 70, null, 32, null, null, 40, 87, 31, 67, null})),
            new Chunk(IntegerBlock.of(new Integer[] {
                87, 40, 90, null, null, 15, 59, 47, 8, null, 93, 82, null, 47, 3, 37, 76, null, 64, 44, 68, 68, null,
                null, 48, null, 51, 12, 23, 90})),
            new Chunk(IntegerBlock.of(new Integer[] {11, 58, null, null, 63, 95, null, 27, 84, null}))
        ));

        // inner exec and types.
        // types = ImmutableList.of(DataTypes.LongType, DataTypes.IntegerType)
        // for exec returning o_orderkey and o_custkey.
        Executor innerInput = new MockExec(innerTypes, ImmutableList.of(
            new Chunk(
                LongBlock.of(new Long[] {8L, 12L, 29L, null, null, null, 75L, null, 91L, 76L}),
                IntegerBlock.of(new Integer[] {61, 11, null, 91, null, null, 63, null, 92, null})
            ),
            new Chunk(
                LongBlock.of(new Long[] {80L, 22L, 37L, 86L, 43L, 97L, null, 77L, null, null}),
                IntegerBlock.of(new Integer[] {null, null, 52, null, null, 80, 57, null, null, 69})
            ),
            new Chunk(
                LongBlock.of(new Long[] {null, 16L, 90L, 33L, 12L, 17L, 90L, 10L, 74L, null}),
                IntegerBlock.of(new Integer[] {70, 31, null, null, null, 97, null, null, null, null})
            )
        ));

        // join type
        JoinRelType joinType = JoinRelType.LEFT;

        // output type of agg = {group key types + agg function types}
        // count(o_orderkey) group by c_custkey
        List<DataType> outputDataTypes = ImmutableList.of(DataTypes.IntegerType, DataTypes.LongType);
        // output type of join
        // c_custKey, o_orderkey, o_custkey from customer left outer join orders on c_custkey = o_custkey
        // RecordType(INTEGER c_custkey, BIGINT o_orderkey, INTEGER o_custkey)
        List<DataType> joinOutputTypes =
            ImmutableList.of(DataTypes.IntegerType, DataTypes.LongType, DataTypes.IntegerType);

        // depends on agg function
        boolean maxOneRow = false;

        // join keys and join conditions of group join
        // 0 = {EquiJoinKey}
        //  outerIndex = 0
        //  innerIndex = 1
        //  unifiedType = {IntegerType}
        //  nullSafeEqual = false
        // output index = 0 (c_custkey in outer exec)
        // input index = 1 (o_custkey in inner exec, and the 0 slot is o_orderkey)
        List<EquiJoinKey> joinKeys = ImmutableList.of(new EquiJoinKey(0, 1, DataTypes.IntegerType, false));
        IExpression otherCondition = null;

        // group set of agg
        // group set = {0} for group by c_custkey.
        int[] groups = {0};

        // agg functions
        // CountV2 for count(o_orderkey)
        // the aggIndex of (inner types union outer types)
        // target index = 1 for o_orderkey in (c_custkey, o_orderkey, o_custkey)
        int[] targetAggIndex = {1};
        List<Aggregator> aggregators = ImmutableList.of(
            new CountV2(targetAggIndex, false, context.getMemoryPool().getMemoryAllocatorCtx(), -1)
        );

        // estimated output rows
        int expectedOutputRowCount = 1000;

        HashGroupJoinExec hashGroupJoinExec = new HashGroupJoinExec(
            outerInput,
            innerInput,
            joinType,
            outputDataTypes,
            joinOutputTypes,
            maxOneRow,
            joinKeys,
            otherCondition,
            null,
            groups,
            aggregators,
            context,
            expectedOutputRowCount
        );

        SingleExecTest singleExecTest = new SingleExecTest.Builder(hashGroupJoinExec, outerInput).build();
        singleExecTest.exec();

        Chunk expect = new Chunk(
            IntegerBlock.of(new Integer[] {
                11, 63, 69, 70, 31, 68, 81, 0, 35, 90, 38, 77, 76, 0, 0, 32, 0, 0, 40, 87, 67, 0, 87, 40, 90, 0, 0, 15,
                59, 47, 8, 0, 93, 82, 0, 47, 3, 37, 76, 0, 64, 44, 68, 68, 0, 0, 48, 0, 51, 12, 23, 90, 58, 0, 0, 95, 0,
                27, 84, 0}),
            LongBlock.of(new Long[] {
                1L, 1L, 0L, 0L, 1L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
                0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
                0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L})
        );

        assertExecResultByRow(singleExecTest.result(), ImmutableList.of(expect), false);
    }

}
