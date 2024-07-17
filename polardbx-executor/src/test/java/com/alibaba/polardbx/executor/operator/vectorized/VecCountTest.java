package com.alibaba.polardbx.executor.operator.vectorized;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.apache.calcite.sql.SqlKind;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class VecCountTest extends GroupByTestBase {
    @Before
    public void config() {
        Map connectionMap = new HashMap();
        connectionMap.put(ConnectionParams.ENABLE_VEC_ACCUMULATOR.getName(), true);
        context.setParamManager(new ParamManager(connectionMap));
    }

    @Test
    public void testCount() {
        // build input values by row count and input types.
        final int totalRows = 9900;
        final int groupKeyBound = 8;

        // select a, count(*) from table group by a
        doAggTest(
            new DataType[] {DataTypes.IntegerType},  // input DataTypes
            new int[] {0}, // group index
            new int[][] {{0}}, // aggregator index
            new SqlKind[] {SqlKind.COUNT},  // agg kind
            totalRows, groupKeyBound);

        // select a, b, count(*) from table group by a, b
        doAggTest(
            new DataType[] {DataTypes.IntegerType, DataTypes.IntegerType},  // input DataTypes
            new int[] {0, 1}, // group index
            new int[][] {{0}}, // aggregator index
            new SqlKind[] {SqlKind.COUNT},  // agg kind
            totalRows, groupKeyBound);

        // select a, b, c, count(*) from table group by a, b, c
        doAggTest(
            new DataType[] {DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.IntegerType},  // input DataTypes
            new int[] {0, 1, 2}, // group index
            new int[][] {{0}}, // aggregator index
            new SqlKind[] {SqlKind.COUNT},  // agg kind
            totalRows, groupKeyBound);

        // select a, b, c, d, count(*) from table group by a, b, c, d
        doAggTest(
            // input DataTypes
            new DataType[] {DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.IntegerType},
            new int[] {0, 1, 2, 3}, // group index
            new int[][] {{0}}, // aggregator index
            new SqlKind[] {SqlKind.COUNT},  // agg kind
            // use the less distinct value.
            totalRows, groupKeyBound / 2);

        // select a, b, c, d, e, count(*) from table group by a, b, c, d, e
        doAggTest(
            // input DataTypes
            new DataType[] {
                DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.IntegerType,
                DataTypes.IntegerType},
            new int[] {0, 1, 2, 3, 4}, // group index
            new int[][] {{0}}, // aggregator index
            new SqlKind[] {SqlKind.COUNT},  // agg kind
            // use the less distinct value.
            totalRows, groupKeyBound / 3);
    }
}
