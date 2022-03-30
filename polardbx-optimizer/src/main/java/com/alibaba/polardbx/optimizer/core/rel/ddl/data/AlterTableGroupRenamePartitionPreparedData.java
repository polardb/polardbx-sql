package com.alibaba.polardbx.optimizer.core.rel.ddl.data;

import com.alibaba.polardbx.common.utils.Pair;

import java.util.ArrayList;
import java.util.List;

public class AlterTableGroupRenamePartitionPreparedData extends DdlPreparedData {

    public AlterTableGroupRenamePartitionPreparedData() {
    }

    private String tableGroupName;

    private List<Pair<String, String>> changePartitionsPair;
    private String sourceSql;

    public String getTableGroupName() {
        return tableGroupName;
    }

    public void setTableGroupName(String tableGroupName) {
        this.tableGroupName = tableGroupName;
    }

    public List<Pair<String, String>> getChangePartitionsPair() {
        return changePartitionsPair;
    }

    public void setChangePartitionsPair(
        List<Pair<String, String>> changePartitionsPair) {
        this.changePartitionsPair = changePartitionsPair;
    }

    public String getSourceSql() {
        return sourceSql;
    }

    public void setSourceSql(String sourceSql) {
        this.sourceSql = sourceSql;
    }

    public List<String> getRelatedPartitions() {
        List<String> relatedParts = new ArrayList<>();
        for (Pair<String, String> changePair : changePartitionsPair) {
            relatedParts.add(changePair.getKey());
            relatedParts.add(changePair.getValue());
        }

        return relatedParts;
    }
}
