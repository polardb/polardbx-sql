package com.alibaba.polardbx.executor.operator.frame;

import com.alibaba.polardbx.executor.operator.util.ChunksIndex;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;

import java.io.Serializable;
import java.util.List;

public interface OverWindowFrame extends Serializable {

    List<Aggregator> getAggregators();

    // 更新该partition涉及的chunks
    void resetChunks(ChunksIndex chunksIndex);

    // 更新该partition在chunksIndex中的边界索引
    void updateIndex(int leftIndex, int rightIndex);

    // 调用window frame处理当前行
    List<Object> processData(int index);
}

