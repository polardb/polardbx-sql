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

public class VecMinMaxTest extends GroupByTestBase {
    @Before
    public void config() {
        Map connectionMap = new HashMap();
        connectionMap.put(ConnectionParams.ENABLE_VEC_ACCUMULATOR.getName(), true);
        context.setParamManager(new ParamManager(connectionMap));
    }

    @Test
    public void testMin() {
        // metadata
        final DataType[] inputDataTypes = {DataTypes.IntegerType, DataTypes.IntegerType};
        final int[] groups = {0};
        final int[][] aggregatorIndexes = {{1}};
        final SqlKind[] aggKinds = {SqlKind.MIN};

        // build input values by row count and input types.
        int totalRows = 9900;
        int groupKeyBound = 10;
        doAggTest(inputDataTypes, groups, aggregatorIndexes, aggKinds, totalRows, groupKeyBound);
    }

    @Test
    public void testMax() {
        // metadata
        final DataType[] inputDataTypes = {DataTypes.IntegerType, DataTypes.IntegerType};
        final int[] groups = {0};
        final int[][] aggregatorIndexes = {{1}};
        final SqlKind[] aggKinds = {SqlKind.MAX};

        // build input values by row count and input types.
        int totalRows = 9900;
        int groupKeyBound = 10;
        doAggTest(inputDataTypes, groups, aggregatorIndexes, aggKinds, totalRows, groupKeyBound);
    }

}
