package com.alibaba.polardbx.executor.operator.vectorized;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.DecimalType;
import org.apache.calcite.sql.SqlKind;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class VecSumAvgTest extends GroupByTestBase {
    @Before
    public void config() {
        Map connectionMap = new HashMap();
        connectionMap.put(ConnectionParams.ENABLE_VEC_ACCUMULATOR.getName(), true);
        context.setParamManager(new ParamManager(connectionMap));
    }

    @Test
    public void testSumHighCardinality() {
        // build input values by row count and input types.
        int totalRows = 5500;
        int groupKeyBound = 1024;
        doAggTest(
            // input data types
            new DataType[] {DataTypes.IntegerType, new DecimalType(15, 2)},
            // group indexes
            new int[] {0},
            // agg indexes
            new int[][] {{1}},
            // agg function
            new SqlKind[] {SqlKind.SUM},
            totalRows, groupKeyBound);
    }

    @Test
    public void testSumInteger() {
        // build input values by row count and input types.
        int totalRows = 9900;
        int groupKeyBound = 8;
        doAggTest(
            // input data types
            new DataType[] {DataTypes.IntegerType, DataTypes.IntegerType},
            // group indexes
            new int[] {0},
            // agg indexes
            new int[][] {{1}},
            // agg function
            new SqlKind[] {SqlKind.SUM},
            totalRows, groupKeyBound);

        doAggTest(
            // input data types
            new DataType[] {DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.IntegerType},
            // group indexes
            new int[] {0, 1},
            // agg indexes
            new int[][] {{2}},
            // agg function
            new SqlKind[] {SqlKind.SUM},
            // reduce cardinality in group-by
            totalRows, groupKeyBound / 2);

        doAggTest(
            // input data types
            new DataType[] {DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.IntegerType},
            // group indexes
            new int[] {0, 1, 2},
            // agg indexes
            new int[][] {{3}},
            // agg function
            new SqlKind[] {SqlKind.SUM},
            // reduce cardinality in group-by
            totalRows, groupKeyBound / 3);
    }

    @Test
    public void testSumLong() {
        // build input values by row count and input types.
        int totalRows = 9900;
        int groupKeyBound = 8;

        doAggTest(
            // input data types
            new DataType[] {DataTypes.LongType, DataTypes.IntegerType, DataTypes.LongType, DataTypes.LongType},
            // group indexes
            new int[] {0, 1, 2},
            // agg indexes
            new int[][] {{3}},
            // agg function
            new SqlKind[] {SqlKind.SUM},
            // reduce cardinality in group-by
            totalRows, groupKeyBound / 3);
    }

    @Test
    public void testSum3() {
        // build input values by row count and input types.
        int totalRows = 9900;
        int groupKeyBound = 8;

        doAggTest(
            // input data types
            new DataType[] {DataTypes.IntegerType, new DecimalType(18, 2)},
            // group indexes
            new int[] {0},
            // agg indexes
            new int[][] {{1}},
            // agg function
            new SqlKind[] {SqlKind.SUM},
            // reduce cardinality in group-by
            totalRows, groupKeyBound);

        doAggTest(
            // input data types
            new DataType[] {DataTypes.LongType, new DecimalType(18, 2)},
            // group indexes
            new int[] {0},
            // agg indexes
            new int[][] {{1}},
            // agg function
            new SqlKind[] {SqlKind.SUM},
            // reduce cardinality in group-by
            totalRows, groupKeyBound);

        doAggTest(
            // input data types
            new DataType[] {DataTypes.IntegerType, DataTypes.IntegerType, new DecimalType(18, 2)},
            // group indexes
            new int[] {0, 1},
            // agg indexes
            new int[][] {{2}},
            // agg function
            new SqlKind[] {SqlKind.SUM},
            // reduce cardinality in group-by
            totalRows, groupKeyBound);
    }

    @Test
    public void testAvg() {
        // metadata
        final DataType[] inputDataTypes = {DataTypes.IntegerType, DataTypes.IntegerType};
        final int[] groups = {0};
        final int[][] aggregatorIndexes = {{1}};
        final SqlKind[] aggKinds = {SqlKind.AVG};

        // build input values by row count and input types.
        int totalRows = 9900;
        int groupKeyBound = 10;
        doAggTest(inputDataTypes, groups, aggregatorIndexes, aggKinds, totalRows, groupKeyBound);
    }
}
