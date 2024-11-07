package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;

import java.util.List;

/**
 * Used to backfill data from base table to index table when a secondary global
 * index is created.
 */
public class GsiPartitionBackfill extends GsiBackfill {

    public List<String> getPartitionList() {
        return partitionList;
    }

    public void setPartitionList(List<String> partitionList) {
        this.partitionList = partitionList;
    }

    private List<String> partitionList = null;

    public static GsiPartitionBackfill createGsiPartitionBackfill(String schemaName, String baseTableName,
                                                                  String indexName,
                                                                  ExecutionContext ec) {
        final RelOptCluster cluster = SqlConverter.getInstance(schemaName, ec).createRelOptCluster(null);
        RelTraitSet traitSet = RelTraitSet.createEmpty();
        return new GsiPartitionBackfill(cluster, traitSet, schemaName, baseTableName, indexName, null);
    }

    public GsiPartitionBackfill(RelOptCluster cluster, RelTraitSet traitSet, String schemaName, String baseTableName,
                                String indexName, List<String> columns) {
        super(cluster, traitSet, schemaName, baseTableName, indexName);
    }

}
