package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.chunk.StringBlock;
import com.alibaba.polardbx.executor.operator.util.EquiJoinMockData;
import com.alibaba.polardbx.executor.operator.util.RowChunksBuilder;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.core.JoinRelType;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.executor.operator.util.RowChunksBuilder.rowChunksBuilder;

public class VecHashJoinTest extends HashJoinTest {
    @Before
    public void before() {
        Map connectionMap = new HashMap();
        connectionMap.put(ConnectionParams.CHUNK_SIZE.getName(), 1024);

        // open vectorization implementation o f join probing and rows building.
        connectionMap.put(ConnectionParams.ENABLE_VEC_JOIN.getName(), true);
        connectionMap.put(ConnectionParams.ENABLE_VEC_BUILD_JOIN_ROW.getName(), true);
        context.setParamManager(new ParamManager(connectionMap));
    }
}
