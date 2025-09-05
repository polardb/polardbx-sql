package com.alibaba.polardbx.common.oss;

import com.alibaba.polardbx.common.utils.Pair;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class ColumnarPartitionPrunedSnapshot {
    private final List<Pair<String, Long>> orcFilesAndSchemaTs;
    private final List<Pair<String, Pair<Long, Long>>> csvFilesAndSchemaTsWithPos;

    public ColumnarPartitionPrunedSnapshot() {
        this.orcFilesAndSchemaTs = new ArrayList<>();
        this.csvFilesAndSchemaTsWithPos = new ArrayList<>();
    }
}
