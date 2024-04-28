package com.alibaba.polardbx.executor.operator.frame;

import com.alibaba.polardbx.executor.operator.util.ChunksIndex;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The sliding window frame.
 */
public abstract class SlidingOverFrame implements OverWindowFrame {

    protected List<Aggregator> aggregators;
    protected ChunksIndex chunksIndex;

    // 保存上次处理的当前行的范围，如果完全一致则可直接返回结果，无需重复计算
    // updateIndex时重置，即每计算一个新的partition时重置
    protected int prevLeftIndex = -1;
    protected int prevRightIndex = -1;

    public SlidingOverFrame(List<Aggregator> aggregator) {
        this.aggregators = aggregator;
    }

    @Override
    public void resetChunks(ChunksIndex chunksIndex) {
        this.chunksIndex = chunksIndex;
    }

    @Override
    public List<Aggregator> getAggregators() {
        return aggregators;
    }

    public List<Object> process(int leftIndex, int rightIndex) {
        if (leftIndex == prevLeftIndex && rightIndex == prevRightIndex) {
            return aggregators.stream().map(t -> t.value()).collect(Collectors.toList());
        }
        prevLeftIndex = leftIndex;
        prevRightIndex = rightIndex;
        final List<Aggregator> newAggregators = new ArrayList<>(aggregators.size());
        List<Object> collect = aggregators.stream().map(t -> {
            Aggregator newAggregator = t.getNew();
            newAggregators.add(newAggregator);
            for (int i = leftIndex; i <= rightIndex; i++) {
                newAggregator.aggregate(chunksIndex.rowAt(i));
            }

            return newAggregator.value();
        }).collect(Collectors.toList());
        aggregators = newAggregators;
        return collect;
    }
}
