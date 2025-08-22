package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.executor.mpp.split.JdbcSplit;
import com.alibaba.polardbx.executor.mpp.split.ParamsDynamicJdbcSplit;

import java.util.List;

public interface MultiUnionAllLookupTableExec extends LookupTableExec {

    @Override
    default boolean shardEnabled() {
        return false;
    }

    default void updateMultiValueWhereSql(Chunk chunk, List<Split> reservedSplits, TableScanClient scanClient) {
        long reserveSize = 0;
        for (Split split : reservedSplits) {
            JdbcSplit jdbcSplit = (JdbcSplit) split.getConnectorSplit();
            ParamsDynamicJdbcSplit dynamicSplit = new ParamsDynamicJdbcSplit(jdbcSplit, chunk, false);
            dynamicSplit.setLimit(jdbcSplit.getLimit());
            reserveSize += jdbcSplit.getSqlTemplate().size() * chunk.getPositionCount();
            scanClient.addSplit(split.copyWithSplit(dynamicSplit));
        }
        reserveMemory(reserveSize);
    }

    default void updateSingleValueWhereSql(Chunk chunk, List<Split> reservedSplits, TableScanClient scanClient) {
        long reserveSize = 0;
        for (Split split : reservedSplits) {
            JdbcSplit jdbcSplit = (JdbcSplit) split.getConnectorSplit();
            ParamsDynamicJdbcSplit dynamicSplit = new ParamsDynamicJdbcSplit(
                jdbcSplit, chunk, true);
            dynamicSplit.setLimit(jdbcSplit.getLimit());
            dynamicSplit.setLimit(jdbcSplit.getLimit());
            reserveSize += jdbcSplit.getSqlTemplate().size() * chunk.getPositionCount();
            scanClient.addSplit(split.copyWithSplit(dynamicSplit));
        }
        reserveMemory(reserveSize);
    }

    void reserveMemory(long reservedSize);
}
