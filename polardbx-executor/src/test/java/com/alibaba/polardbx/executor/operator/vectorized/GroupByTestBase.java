package com.alibaba.polardbx.executor.operator.vectorized;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.DecimalRoundMod;
import com.alibaba.polardbx.common.datatype.DecimalTypeBase;
import com.alibaba.polardbx.common.datatype.FastDecimalUtils;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.DecimalBlock;
import com.alibaba.polardbx.executor.chunk.DecimalBlockBuilder;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.operator.BaseExecTest;
import com.alibaba.polardbx.executor.operator.Executor;
import com.alibaba.polardbx.executor.operator.HashAggExec;
import com.alibaba.polardbx.executor.operator.MockExec;
import com.alibaba.polardbx.executor.operator.SingleExecTest;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.DecimalType;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.AvgV2;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.CountV2;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.FirstValue;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.MaxV2;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.MinV2;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.SumV2;
import com.alibaba.polardbx.optimizer.core.row.Row;
import org.apache.calcite.sql.SqlKind;
import org.junit.Assert;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

public class GroupByTestBase extends BaseExecTest {
    // Use a small hash table to invoke rehash.
    protected static final int DEFAULT_EXPECTED_GROUPS = 2;
    protected static final Random RANDOM = new Random();

    protected void doAggTest(DataType[] inputDataTypes, int[] groups, int[][] aggregatorIndexes, SqlKind[] aggKinds,
                             int totalRows, int groupKeyBound) {
        int chunkSize = context.getParamManager().getInt(ConnectionParams.CHUNK_SIZE);

        List<Chunk> inputChunks = new ArrayList<>();
        for (int currentPosition = 0; currentPosition < totalRows; ) {

            int positionCount = Math.min(chunkSize, totalRows - currentPosition);

            // build blocks by input types.
            Block[] blocks = new Block[inputDataTypes.length];
            int groupIndex = 0;
            for (int blockIndex = 0; blockIndex < inputDataTypes.length; blockIndex++) {
                final DataType blockType = inputDataTypes[blockIndex];
                switch (blockType.fieldType()) {
                case MYSQL_TYPE_LONG:
                    if (groupIndex < groups.length && groups[groupIndex] == blockIndex) {
                        // random int value between 0 and group key bound
                        blocks[blockIndex] = randomIntegerBlock(positionCount, 0, groupKeyBound);
                        groupIndex++;
                    } else {
                        // random int value between 0 and 1000000
                        blocks[blockIndex] = randomIntegerBlock(positionCount, 0, 1000000);
                    }
                    break;
                case MYSQL_TYPE_LONGLONG:
                    if (groupIndex < groups.length && groups[groupIndex] == blockIndex) {
                        // random int value between 0 and group key bound
                        blocks[blockIndex] = randomLongBlock(positionCount, 0, groupKeyBound);
                        groupIndex++;
                    } else {
                        // random long value between 0 and 100000000
                        blocks[blockIndex] = randomLongBlock(positionCount, 0, 100000000);
                    }
                    break;
                case MYSQL_TYPE_DECIMAL:
                case MYSQL_TYPE_NEWDECIMAL:
                    if (groupIndex < groups.length && groups[groupIndex] == blockIndex) {
                        // random int value between 0 and group key bound
                        blocks[blockIndex] = randomDecimalBlock(10, 0, positionCount, 0, groupKeyBound);
                        groupIndex++;
                    } else {
                        // random long value between 0 and 100000000
                        blocks[blockIndex] = randomDecimalBlock(blockType.getPrecision(), blockType.getScale(),
                            positionCount, 0, 100000000);
                    }
                    break;
                default:
                    throw new UnsupportedOperationException();
                }
            }
            Chunk chunk = new Chunk(positionCount, blocks);
            inputChunks.add(chunk);
            currentPosition += positionCount;
        }

        // build input exec
        MockExec.MockExecBuilder execBuilder = MockExec.builder(inputDataTypes);
        for (Chunk inputChunk : inputChunks) {
            execBuilder.withChunk(inputChunk);
        }
        Executor inputExec = execBuilder.build();

        // Get output columns type by agg kind and group by columns
        List<DataType> outputTypes = new ArrayList<>();
        for (int i = 0; i < groups.length; i++) {
            DataType groupKeyType = inputDataTypes[groups[0]];
            outputTypes.add(groupKeyType);
        }
        for (int i = 0; i < aggKinds.length; i++) {
            int[] aggIndexes = aggregatorIndexes[i];
            SqlKind aggKind = aggKinds[i];

            switch (aggKind) {
            case SUM:
            case AVG:
                // decimal type for sum/avg
                DataType inputType = inputDataTypes[aggIndexes[0]];
                if (inputType instanceof DecimalType) {
                    outputTypes.add(inputType);
                } else {
                    outputTypes.add(DataTypes.DecimalType);
                }
                break;
            case COUNT:
                // long type
                outputTypes.add(DataTypes.LongType);
                break;
            case FIRST_VALUE:
            case MIN:
            case MAX:
                // be same to agg input value type.
                outputTypes.add(inputDataTypes[aggIndexes[0]]);
                break;
            default:
                throw new UnsupportedOperationException();
            }
        }

        // build aggregators by agg kind.
        List<Aggregator> aggregators = new ArrayList<>();
        for (int i = 0; i < aggKinds.length; i++) {
            SqlKind aggKind = aggKinds[i];
            int[] aggIndex = aggregatorIndexes[i];
            Aggregator aggregator = null;
            switch (aggKind) {
            case SUM:
                aggregator = new SumV2(aggIndex[0], false, context.getMemoryPool().getMemoryAllocatorCtx(), -1);
                break;
            case COUNT:
                aggregator = new CountV2(aggIndex, false, context.getMemoryPool().getMemoryAllocatorCtx(), -1);
                break;
            case AVG:
                aggregator = new AvgV2(aggIndex[0], false, context.getMemoryPool().getMemoryAllocatorCtx(), -1);
                break;
            case FIRST_VALUE:
                aggregator = new FirstValue(aggIndex[0], -1);
                break;
            case MAX:
                aggregator = new MaxV2(aggIndex[0], -1);
                break;
            case MIN:
                aggregator = new MinV2(aggIndex[0], -1);
                break;
            default:
                throw new UnsupportedOperationException();
            }
            aggregators.add(aggregator);
        }

        // Build hash agg exec, driver exec, and invoke execution.
        HashAggExec exec = new HashAggExec(inputExec.getDataTypes(), groups,
            aggregators, outputTypes, DEFAULT_EXPECTED_GROUPS, context);

        SingleExecTest testDriver = new SingleExecTest.Builder(exec, ((MockExec) inputExec).getChunks()).build();
        testDriver.exec();

        // Check the results
        System.out.println("actual results (calculated): ");
        List<Chunk> resultChunks = testDriver.result();
        for (Chunk chunk : resultChunks) {
            for (int i = 0; i < chunk.getPositionCount(); i++) {
                Row row = chunk.rowAt(i);
                System.out.println(row);
            }
        }

        // calculate agg results by other method.
        Map<List, Object[]> aggResults =
            getResults(inputDataTypes, groups, aggregatorIndexes, aggKinds, totalRows, inputChunks, outputTypes);

        for (Chunk chunk : resultChunks) {
            for (int position = 0; position < chunk.getPositionCount(); position++) {
                List groupByList = new ArrayList();

                // the blocks for group by
                for (int groupIndex = 0; groupIndex < groups.length; groupIndex++) {
                    groupByList.add(chunk.getBlock(groups[groupIndex]).getObject(position));
                }

                Object[] aggResult = aggResults.get(groupByList);
                Assert.assertTrue(aggResult != null);

                // the blocks for accumulation result
                int aggIndex = 0;
                try {
                    for (int i = groups.length; i < outputTypes.size(); i++) {
                        Object actualAggResult = chunk.getBlock(i).getObject(position);
                        Object expectedResult = ((List) aggResult[1]).get(aggIndex++);
                        Assert.assertEquals(expectedResult, actualAggResult);
                    }
                } catch (Throwable t) {
                    // dump
//                    for (Chunk c : inputChunks) {
//                        for (int i = 0; i < c.getPositionCount(); i++) {
//                            Row row = c.rowAt(i);
//                            System.out.println(row);
//                        }
//                    }
                    throw t;
                }

            }
        }

        List<List> groupByLists = new ArrayList<>();
        for (int i = 0; i < totalRows; i++) {
            groupByLists.add(new ArrayList());
        }
        for (int groupIndex = 0; groupIndex < groups.length; groupIndex++) {
            DataType inputType = inputDataTypes[groups[groupIndex]];
            switch (inputType.fieldType()) {
            case MYSQL_TYPE_LONG: {
                int groupByCount = 0;
                for (Chunk inputChunk : inputChunks) {
                    for (int position = 0; position < inputChunk.getPositionCount(); position++) {
                        int intValue = inputChunk.getBlock(groups[groupIndex]).getInt(position);
                        groupByLists.get(groupByCount++).add(intValue);
                    }
                }
                break;
            }
            case MYSQL_TYPE_LONGLONG: {
                int groupByCount = 0;
                for (Chunk inputChunk : inputChunks) {
                    for (int position = 0; position < inputChunk.getPositionCount(); position++) {
                        long longValue = inputChunk.getBlock(groups[groupIndex]).getLong(position);
                        groupByLists.get(groupByCount++).add(longValue);
                    }
                }
                break;
            }
            default:
                throw new UnsupportedOperationException();
            }
        }
    }

    /**
     * @return {group by list} -> [{count}, {agg results}]
     */
    protected Map<List, Object[]> getResults(DataType[] inputDataTypes, int[] groups, int[][] aggregatorIndexes,
                                             SqlKind[] aggKinds,
                                             int totalRows, List<Chunk> inputChunks, List<DataType> outputTypes) {
        Map<List, Object[]> aggResults = new HashMap<>();
        // collect all group by list.
        List<List> groupByLists = new ArrayList<>();
        for (int i = 0; i < totalRows; i++) {
            groupByLists.add(new ArrayList());
        }
        for (int groupIndex = 0; groupIndex < groups.length; groupIndex++) {
            DataType inputType = inputDataTypes[groups[groupIndex]];
            switch (inputType.fieldType()) {
            case MYSQL_TYPE_LONG: {
                int groupByCount = 0;
                for (Chunk inputChunk : inputChunks) {
                    for (int position = 0; position < inputChunk.getPositionCount(); position++) {
                        int intValue = inputChunk.getBlock(groups[groupIndex]).getInt(position);
                        groupByLists.get(groupByCount++).add(intValue);
                    }
                }
                break;
            }
            case MYSQL_TYPE_LONGLONG: {
                int groupByCount = 0;
                for (Chunk inputChunk : inputChunks) {
                    for (int position = 0; position < inputChunk.getPositionCount(); position++) {
                        long longValue = inputChunk.getBlock(groups[groupIndex]).getLong(position);
                        groupByLists.get(groupByCount++).add(longValue);
                    }
                }
                break;
            }
            default:
                throw new UnsupportedOperationException();
            }
        }

        // prepare agg results map
        for (List groupByList : groupByLists) {
            if (!aggResults.containsKey(groupByList)) {
                // initial this group
                aggResults.put(groupByList, new Object[] {1, new ArrayList()});
            } else {
                // accumulation
                Object[] value = aggResults.get(groupByList);
                value[0] = ((int) value[0]) + 1;
            }
        }

        for (List groupByList : aggResults.keySet()) {
            List aggResult = (List) (aggResults.get(groupByList)[1]);
            int aggIndex = 0;
            for (int i = groups.length; i < outputTypes.size(); i++) {
                SqlKind aggKind = aggIndex >= aggKinds.length ? null : aggKinds[aggIndex++];
                switch (outputTypes.get(i).fieldType()) {
                case MYSQL_TYPE_DECIMAL:
                case MYSQL_TYPE_NEWDECIMAL:
                    // Initial value for different decimal results.
                    if (aggKind != null) {
                        switch (aggKind) {
                        case FIRST_VALUE:
                            aggResult.add(Optional.empty());
                            break;
                        case MIN:
                            aggResult.add(Decimal.fromLong(Long.MAX_VALUE));
                            break;
                        case MAX:
                            aggResult.add(Decimal.fromLong(Long.MIN_VALUE));
                            break;
                        default:
                            aggResult.add(Decimal.fromLong(0));
                        }
                    } else {
                        aggResult.add(Decimal.fromLong(0));
                    }
                    break;
                case MYSQL_TYPE_LONG: {
                    // Initial value for different long results.
                    if (aggKind != null) {
                        switch (aggKind) {
                        case FIRST_VALUE:
                            aggResult.add(Optional.empty());
                            break;
                        case MIN:
                            aggResult.add(Integer.MAX_VALUE);
                            break;
                        case MAX:
                            aggResult.add(Integer.MIN_VALUE);
                            break;
                        default:
                            aggResult.add(0);
                        }
                    } else {
                        aggResult.add(0);
                    }
                    break;
                }
                case MYSQL_TYPE_LONGLONG: {
                    // Initial value for different long results.
                    if (aggKind != null) {
                        switch (aggKind) {
                        case FIRST_VALUE:
                            aggResult.add(Optional.empty());
                            break;
                        case MIN:
                            aggResult.add(Long.MAX_VALUE);
                            break;
                        case MAX:
                            aggResult.add(Long.MIN_VALUE);
                            break;
                        default:
                            aggResult.add(0L);
                        }
                    } else {
                        aggResult.add(0L);
                    }
                    break;
                }

                }
            }
        }

        for (int aggIndex = 0; aggIndex < aggKinds.length; aggIndex++) {
            SqlKind aggKind = aggKinds[aggIndex];
            int inputIndex = aggregatorIndexes[aggIndex][0];
            DataType inputType = inputDataTypes[inputIndex];

            switch (aggKind) {
            case SUM:
            case AVG: {
                switch (inputType.fieldType()) {
                case MYSQL_TYPE_LONG: {
                    // sum(int) -> decimal
                    // avg(int) -> decimal

                    int groupByCount = 0;
                    for (Chunk inputChunk : inputChunks) {
                        for (int position = 0; position < inputChunk.getPositionCount(); position++) {
                            int intValue = inputChunk.getBlock(inputIndex).getInt(position);
                            List groupBy = groupByLists.get(groupByCount++);
                            Decimal oldValue = (Decimal) ((List) aggResults.get(groupBy)[1]).get(aggIndex);
                            Decimal newValue = oldValue.add(Decimal.fromLong(intValue));
                            ((List) aggResults.get(groupBy)[1]).set(aggIndex, newValue);
                        }
                    }
                    break;
                }
                case MYSQL_TYPE_LONGLONG: {
                    // sum(long) -> decimal
                    // avg(long) -> decimal

                    int groupByCount = 0;
                    for (Chunk inputChunk : inputChunks) {
                        for (int position = 0; position < inputChunk.getPositionCount(); position++) {
                            long longValue = inputChunk.getBlock(inputIndex).getLong(position);
                            List groupBy = groupByLists.get(groupByCount++);
                            Decimal oldValue = (Decimal) ((List) aggResults.get(groupBy)[1]).get(aggIndex);
                            Decimal newValue = oldValue.add(Decimal.fromLong(longValue));
                            ((List) aggResults.get(groupBy)[1]).set(aggIndex, newValue);
                        }
                    }
                    break;
                }
                case MYSQL_TYPE_DECIMAL:
                case MYSQL_TYPE_NEWDECIMAL: {
                    // sum(decimal) -> decimal
                    // avg(decimal) -> decimal

                    int groupByCount = 0;
                    for (Chunk inputChunk : inputChunks) {
                        for (int position = 0; position < inputChunk.getPositionCount(); position++) {
                            Decimal decimalVal = inputChunk.getBlock(inputIndex).getDecimal(position);
                            List groupBy = groupByLists.get(groupByCount++);
                            Decimal oldValue = (Decimal) ((List) aggResults.get(groupBy)[1]).get(aggIndex);
                            Decimal newValue = oldValue.add(decimalVal);
                            ((List) aggResults.get(groupBy)[1]).set(aggIndex, newValue);
                        }
                    }
                    break;
                }
                default:
                    throw new UnsupportedOperationException();
                }

                // for avg function, divide count in agg index.
                if (aggKind == SqlKind.AVG) {
                    final int finalAggIndex = aggIndex;
                    aggResults.forEach((groupBy, aggResult) -> {
                        int count = (int) aggResult[0];
                        Decimal sum = (Decimal) ((List) aggResult[1]).get(finalAggIndex);
                        Decimal result = sum.divide(Decimal.fromLong(count));
                        Decimal round = new Decimal();
                        FastDecimalUtils.round(result.getDecimalStructure(), round.getDecimalStructure(),
                            DecimalTypeBase.DEFAULT_DIV_PRECISION_INCREMENT, DecimalRoundMod.HALF_UP);

                        ((List) aggResult[1]).set(finalAggIndex, round);
                    });
                }

                break;
            }
            case COUNT: {
                // count(any) -> long
                // for count function, set count as long value in agg index.
                final int finalAggIndex = aggIndex;
                aggResults.forEach((groupBy, aggResult) -> {
                    int count = (int) aggResult[0];
                    ((List) aggResult[1]).set(finalAggIndex, Long.valueOf(count));
                });
                break;
            }
            case FIRST_VALUE: {
                switch (inputType.fieldType()) {
                case MYSQL_TYPE_LONG: {
                    // first_value(int) -> int
                    // first_value(long) -> long
                    // first_value(decimal) -> decimal

                    int groupByCount = 0;
                    for (Chunk inputChunk : inputChunks) {
                        for (int position = 0; position < inputChunk.getPositionCount(); position++) {
                            int intValue = inputChunk.getBlock(inputIndex).getInt(position);
                            List groupBy = groupByLists.get(groupByCount++);
                            Object oldValue = ((List) aggResults.get(groupBy)[1]).get(aggIndex);
                            Object newValue = oldValue instanceof Optional && !((Optional<?>) oldValue).isPresent()
                                ? Integer.valueOf(intValue) : oldValue;

                            ((List) aggResults.get(groupBy)[1]).set(aggIndex, newValue);
                        }
                    }
                    break;
                }
                case MYSQL_TYPE_LONGLONG: {
                    // sum(long) -> decimal
                    // avg(long) -> decimal

                    int groupByCount = 0;
                    for (Chunk inputChunk : inputChunks) {
                        for (int position = 0; position < inputChunk.getPositionCount(); position++) {
                            long longValue = inputChunk.getBlock(inputIndex).getLong(position);
                            List groupBy = groupByLists.get(groupByCount++);
                            Object oldValue = ((List) aggResults.get(groupBy)[1]).get(aggIndex);
                            Object newValue = oldValue instanceof Optional && !((Optional<?>) oldValue).isPresent()
                                ? longValue : oldValue;

                            ((List) aggResults.get(groupBy)[1]).set(aggIndex, newValue);
                        }
                    }
                    break;
                }
                case MYSQL_TYPE_DECIMAL:
                case MYSQL_TYPE_NEWDECIMAL: {
                    // sum(decimal) -> decimal
                    // avg(decimal) -> decimal

                    int groupByCount = 0;
                    for (Chunk inputChunk : inputChunks) {
                        for (int position = 0; position < inputChunk.getPositionCount(); position++) {
                            Decimal decimalVal = inputChunk.getBlock(inputIndex).getDecimal(position);
                            List groupBy = groupByLists.get(groupByCount++);
                            Object oldValue = ((List) aggResults.get(groupBy)[1]).get(aggIndex);
                            Object newValue = oldValue instanceof Optional && !((Optional<?>) oldValue).isPresent()
                                ? decimalVal : oldValue;

                            ((List) aggResults.get(groupBy)[1]).set(aggIndex, newValue);
                        }
                    }
                    break;
                }
                default:
                    throw new UnsupportedOperationException();
                }
                break;
            }
            case MAX:
            case MIN: {
                // max(int) -> int
                // max(long) -> long
                // max(decimal) -> decimal
                // min(int) -> int
                // min(long) -> long
                // min(decimal) -> decimal
                switch (inputType.fieldType()) {

                case MYSQL_TYPE_LONG: {
                    int groupByCount = 0;
                    for (Chunk inputChunk : inputChunks) {
                        for (int position = 0; position < inputChunk.getPositionCount(); position++) {
                            int intValue = inputChunk.getBlock(inputIndex).getInt(position);
                            List groupBy = groupByLists.get(groupByCount++);
                            Object oldValue = ((List) aggResults.get(groupBy)[1]).get(aggIndex);
                            Object newValue = aggKind == SqlKind.MIN
                                ? Math.min((Integer) oldValue, intValue)
                                : Math.max((Integer) oldValue, intValue);

                            ((List) aggResults.get(groupBy)[1]).set(aggIndex, newValue);
                        }
                    }
                    break;
                }
                case MYSQL_TYPE_LONGLONG: {

                    int groupByCount = 0;
                    for (Chunk inputChunk : inputChunks) {
                        for (int position = 0; position < inputChunk.getPositionCount(); position++) {
                            long longValue = inputChunk.getBlock(inputIndex).getLong(position);
                            List groupBy = groupByLists.get(groupByCount++);
                            Object oldValue = ((List) aggResults.get(groupBy)[1]).get(aggIndex);
                            Object newValue = aggKind == SqlKind.MIN
                                ? Math.min((Long) oldValue, longValue)
                                : Math.max((Long) oldValue, longValue);

                            ((List) aggResults.get(groupBy)[1]).set(aggIndex, newValue);
                        }
                    }
                    break;
                }
                case MYSQL_TYPE_DECIMAL:
                case MYSQL_TYPE_NEWDECIMAL: {

                    int groupByCount = 0;
                    for (Chunk inputChunk : inputChunks) {
                        for (int position = 0; position < inputChunk.getPositionCount(); position++) {
                            Decimal decimalVal = inputChunk.getBlock(inputIndex).getDecimal(position);
                            List groupBy = groupByLists.get(groupByCount++);
                            Object oldValue = ((List) aggResults.get(groupBy)[1]).get(aggIndex);

                            int comparison = ((Decimal) oldValue).compareTo(decimalVal);
                            Decimal upper = comparison <= 0 ? decimalVal : (Decimal) oldValue;
                            Decimal lower = comparison > 0 ? decimalVal : (Decimal) oldValue;
                            Object newValue = aggKind == SqlKind.MIN ? lower : upper;

                            ((List) aggResults.get(groupBy)[1]).set(aggIndex, newValue);
                        }
                    }
                    break;
                }
                default:
                    throw new UnsupportedOperationException();
                }
                break;
            }
            default:
                throw new UnsupportedOperationException();
            }
        }

        System.out.println("expected results (generated): ");
        aggResults.forEach((groupBy, aggResult) -> {
            System.out.println(MessageFormat.format("group: {0} -> count: {1}, agg: {2}",
                groupBy, aggResult[0], aggResult[1]));
        });

        return aggResults;
    }

    protected IntegerBlock randomIntegerBlock(int positionCount, int lowerBound, int upperBound) {
        int[] intArray = RANDOM.ints(positionCount, lowerBound, upperBound).toArray();
        return new IntegerBlock(0, positionCount, null, intArray);
    }

    protected LongBlock randomLongBlock(int positionCount, int lowerBound, int upperBound) {
        long[] longArray = RANDOM.longs(positionCount, lowerBound, upperBound).toArray();
        return new LongBlock(0, positionCount, null, longArray);
    }

    protected DecimalBlock randomDecimalBlock(int precision, int scale, int positionCount, int unscaledLowerBound,
                                              int unscaledUpperBound) {
        long[] longArray = RANDOM.longs(positionCount, unscaledLowerBound, unscaledUpperBound).toArray();

        DecimalType dataType = new DecimalType(precision, scale);
        if (precision < Decimal.MAX_64_BIT_PRECISION) {
            // use decimal 64
            return new DecimalBlock(dataType, positionCount, false, null, longArray, null);
        } else {
            DecimalBlockBuilder blockBuilder = new DecimalBlockBuilder(positionCount);
            for (long longVal : longArray) {
                blockBuilder.writeDecimal(new Decimal(longVal, scale));
            }
            return (DecimalBlock) blockBuilder.build();
        }
    }
}
