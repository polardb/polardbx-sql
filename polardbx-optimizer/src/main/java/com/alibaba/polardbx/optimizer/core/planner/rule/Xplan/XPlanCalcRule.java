package com.alibaba.polardbx.optimizer.core.planner.rule.Xplan;

import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.rel.Xplan.XPlanEqualTuple;
import com.alibaba.polardbx.optimizer.core.rel.Xplan.XPlanTableScan;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.Pair;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @version 1.0
 */
public class XPlanCalcRule extends XPlanNewGetRule {
    IndexInfo indexInfo;

    public XPlanCalcRule(String indexName) {
        super(operand(LogicalFilter.class, operand(XPlanTableScan.class, none())), "XPlan_calc_rule");
        this.indexInfo = new IndexInfo(indexName);
    }

    @Override
    protected int selectIndex(
        XPlanTableScan tableScan,
        TableMeta tableMeta,
        List<Pair<IndexMeta, List<Pair<Integer, Long>>>> lookupIndexes,
        List<XPlanEqualTuple> andConditions) {

        // choose primary key or unique key firstly
        for (int idx = 0; idx < lookupIndexes.size(); ++idx) {
            IndexMeta indexMeta = lookupIndexes.get(idx).getKey();
            // pk or uk
            if (indexMeta.isPrimaryKeyIndex() || indexMeta.isUniqueIndex()) {
                // covering all key columns
                if (indexMeta.getKeyColumns().size() == lookupIndexes.get(idx).getValue().size()) {
                    return idx;
                }
            }
        }

        // Select minimum row count
        Pair<Double, Integer> idx = getBestIndexByRows(tableScan, lookupIndexes, andConditions);
        this.indexInfo.setRowCount(idx.getKey());
        return idx.getValue();
    }

    @Override
    protected List<Pair<IndexMeta, List<Pair<Integer, Long>>>> filterIndex(
        XPlanTableScan tableScan,
        TableMeta tableMeta,
        List<Pair<IndexMeta, List<Pair<Integer, Long>>>> lookupIndexes,
        PlannerContext plannerContext) {
        lookupIndexes = super.filterIndex(tableScan, tableMeta, lookupIndexes, plannerContext);
        this.indexInfo.setTableName(tableMeta.getTableName());
        this.indexInfo.setCandidateIndexes(
            lookupIndexes.stream().map(x -> x.getKey().getPhysicalIndexName()).collect(Collectors.toList()));
        for (Pair<IndexMeta, List<Pair<Integer, Long>>> pair : lookupIndexes) {
            if (pair.getKey().getPhysicalIndexName().equalsIgnoreCase(indexInfo.getIndex())) {
                return ImmutableList.of(pair);
            }
        }
        return lookupIndexes;
    }

    @Override
    protected void recordWhereInfo(RelNode node, boolean usingWhere) {
        RelMetadataQuery mq = node.getCluster().getMetadataQuery();
        this.indexInfo.setFinalRowCount(mq.getRowCount(node));
        this.indexInfo.setUsingWhere(usingWhere);
        this.indexInfo.setFound(true);
    }

    public IndexInfo getIndexInfo() {
        return indexInfo;
    }

    public class IndexInfo {
        List<String> candidateIndexes;
        String index;
        String tableName;
        double rowCount;
        double finalRowCount;
        boolean usingWhere;
        boolean found;

        public IndexInfo(String index) {
            this.index = index;
            this.rowCount = 1D;
            this.finalRowCount = 1D;
            this.usingWhere = false;
            this.found = false;
        }

        public List<String> getCandidateIndexes() {
            return candidateIndexes;
        }

        public void setCandidateIndexes(List<String> candidateIndexes) {
            this.candidateIndexes = candidateIndexes;
        }

        public String getIndex() {
            return index;
        }

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public double getRowCount() {
            return rowCount;
        }

        public void setRowCount(double rowCount) {
            if (rowCount >= 1D) {
                this.rowCount = rowCount;
            }
        }

        public double getFinalRowCount() {
            return finalRowCount;
        }

        public void setFinalRowCount(double finalRowCount) {
            if (finalRowCount >= 1D) {
                this.finalRowCount = finalRowCount;
            }
        }

        public boolean isUsingWhere() {
            return usingWhere;
        }

        public void setUsingWhere(boolean usingWhere) {
            this.usingWhere = usingWhere;
        }

        public boolean isFound() {
            return found;
        }

        public void setFound(boolean found) {
            this.found = found;
        }
    }
}
