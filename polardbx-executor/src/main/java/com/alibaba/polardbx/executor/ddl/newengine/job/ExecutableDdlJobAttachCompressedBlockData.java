package com.alibaba.polardbx.executor.ddl.newengine.job;

import com.alibaba.polardbx.executor.ddl.newengine.dag.DirectedAcyclicGraph;

import java.util.HashMap;
import java.util.Map;

/**
 * ExecutableDdlJob will store in MetaDB, and executed by leader CN
 */
public class ExecutableDdlJobAttachCompressedBlockData extends ExecutableDdlJob {

    private transient final Map<Long, CompressedBlockData> extraBlockValueMap = new HashMap<>();

    public ExecutableDdlJobAttachCompressedBlockData() {
        super();
    }

    public ExecutableDdlJobAttachCompressedBlockData(DirectedAcyclicGraph taskGraph) {
        super(taskGraph);
    }

}
