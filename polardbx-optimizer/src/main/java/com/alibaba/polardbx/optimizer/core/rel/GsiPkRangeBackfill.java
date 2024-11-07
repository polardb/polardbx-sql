package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;

import java.util.List;
import java.util.Map;

/**
 * Used to backfill data from base table to index table when a secondary global
 * index is created.
 */
public class GsiPkRangeBackfill extends GsiBackfill {

    private Pair<Map<Integer, ParameterContext>, Map<Integer, ParameterContext>> pkRange = null;

    public int getTotalThreadCount() {
        return totalThreadCount;
    }

    public void setTotalThreadCount(int totalThreadCount) {
        this.totalThreadCount = totalThreadCount;
    }

    public int totalThreadCount = 1;
    public static GsiPkRangeBackfill createGsiPkRangeBackfill(String schemaName, String baseTableName, String indexName,
                                                              ExecutionContext ec) {
        final RelOptCluster cluster = SqlConverter.getInstance(schemaName, ec).createRelOptCluster(null);
        RelTraitSet traitSet = RelTraitSet.createEmpty();
        return new GsiPkRangeBackfill(cluster, traitSet, schemaName, baseTableName, indexName, null);
    }

    public GsiPkRangeBackfill(RelOptCluster cluster, RelTraitSet traitSet, String schemaName, String baseTableName,
                              String indexName, List<String> columns) {
        super(cluster, traitSet, schemaName, baseTableName, indexName);
    }

    public Pair<Map<Integer, ParameterContext>, Map<Integer, ParameterContext>> getPkRange() {
        return pkRange;
    }

    public void setPkRange(
        Pair<Map<Integer, ParameterContext>, Map<Integer, ParameterContext>> pkRange) {
        this.pkRange = pkRange;
    }
}
