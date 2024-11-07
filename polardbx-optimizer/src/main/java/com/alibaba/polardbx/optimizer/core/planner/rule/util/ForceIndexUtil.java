package com.alibaba.polardbx.optimizer.core.planner.rule.util;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinUtils;
import com.alibaba.polardbx.optimizer.core.join.LookupEquiJoinKey;
import com.alibaba.polardbx.optimizer.core.join.LookupPredicate;
import com.alibaba.polardbx.optimizer.core.join.LookupPredicateBuilder;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.index.TableScanFinder;
import com.alibaba.polardbx.optimizer.utils.DrdsRexFolder;
import com.clearspring.analytics.util.Lists;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.util.Util;
import org.apache.commons.collections.CollectionUtils;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Map;

public class ForceIndexUtil {

    public static String genAutoForceIndex(LogicalView logicalView) {
        TableScan tableScan = getTableScan(logicalView);
        if (tableScan == null) {
            return null;
        }
        TableMeta tm = CBOUtil.getTableMeta(tableScan.getTable());

        // consider bka join column
        ImmutableList<String> bkaColumns = null;
        if (logicalView.getJoin() != null) {
            Join join = logicalView.getJoin();
            List<LookupEquiJoinKey> joinKeys =
                EquiJoinUtils.buildLookupEquiJoinKeys(join, join.getOuter(), join.getInner(),
                    (RexCall) join.getCondition(), join.getJoinType());
            LookupPredicate predicate =
                new LookupPredicateBuilder(join, logicalView.getColumnOrigins()).build(joinKeys);
            ImmutableList.Builder<String> builder = ImmutableList.builder();
            for (int i = 0; i < predicate.size(); i++) {
                builder.add(predicate.getColumn(i).getLastName());
            }
            bkaColumns = builder.build();
        }
        // collect required columns
        RequiredUniqueColumnsCollector requiredUniqueColumnsCollector = new RequiredUniqueColumnsCollector(
            tableScan, logicalView.getCluster().getMetadataQuery(), bkaColumns);
        RequiredColumnsCollector requiredColumnsCollector = new RequiredColumnsCollector(
            tableScan, logicalView.getCluster().getMetadataQuery(), bkaColumns);
        logicalView.getPushedRelNode().accept(requiredUniqueColumnsCollector);
        logicalView.getPushedRelNode().accept(requiredColumnsCollector);

        // get index to be used in force index
        return genAutoForceIndex(requiredUniqueColumnsCollector.build(), requiredColumnsCollector.build(), tm);
    }

    private static TableScan getTableScan(LogicalView logicalView) {
        final ParamManager paramManager = PlannerContext.getPlannerContext(logicalView).getParamManager();
        if (!paramManager.getBoolean(ConnectionParams.ENABLE_AUTO_FORCE_INDEX)) {
            return null;
        }

        if (logicalView instanceof OSSTableScan) {
            return null;
        }

        // should contain only one table
        if (logicalView.getTableNames().size() != 1) {
            return null;
        }
        // logicalView should have no index hint
        RelNode rel = logicalView.getPushedRelNode();
        TableScanFinder tableScanFinder = new TableScanFinder();
        rel.accept(tableScanFinder);
        List<Pair<String, TableScan>> tableScans = tableScanFinder.getResult();
        if (tableScans.size() != 1) {
            return null;
        }
        TableScan tableScan = tableScans.get(0).getValue();
        if (hasIndexHint(tableScan)) {
            return null;
        }
        return tableScan;
    }

    public static boolean hasIndexHint(TableScan tableScan) {
        if (tableScan == null || tableScan.getIndexNode() == null) {
            return false;
        }
        if (tableScan.getIndexNode() instanceof SqlNodeList) {
            SqlNodeList sqlNodeList = (SqlNodeList) tableScan.getIndexNode();
            if (sqlNodeList.size() == 0) {
                return false;
            }
        }
        return true;
    }

    private static String genAutoForceIndex(RequiredUniqueColumns requiredUniqueColumns,
                                            RequiredColumns requiredColumns,
                                            TableMeta tm) {
        if (requiredUniqueColumns != null) {
            // check pk first
            if (requiredUniqueColumns.checkCoverage(tm.getPrimaryIndex())) {
                return "PRIMARY";
            }
            // check uk next
            for (IndexMeta index : tm.getUniqueIndexes(false)) {
                if (requiredUniqueColumns.checkCoverage(index)) {
                    return index.getPhysicalIndexName();
                }
            }
        }

        if (requiredColumns == null) {
            return null;
        }
        // check common key last
        IndexMeta bestIndex = null;
        for (IndexMeta index : tm.getIndexes()) {
            if (requiredColumns.checkIndexCoverage(index)) {
                // get shortest index
                if (bestIndex == null || index.getKeyColumns().size() < bestIndex.getKeyColumns().size()) {
                    bestIndex = index;
                }
            }
        }
        return bestIndex == null ? null : bestIndex.getPhysicalIndexName();
    }

    private static class RequiredUniqueColumnsCollector extends RelShuttleImpl {
        // columns list used in 'equal' or 'in'
        private final List<List<String>> equalColumnsList;
        private final RelMetadataQuery mq;
        private final PlannerContext plannerContext;

        public RequiredUniqueColumnsCollector(TableScan tableScan, RelMetadataQuery mq, List<String> bkaColumns) {
            this.mq = mq;
            this.plannerContext = PlannerContext.getPlannerContext(tableScan);
            this.equalColumnsList = Lists.newArrayList();
            if (CollectionUtils.isNotEmpty(bkaColumns)) {
                equalColumnsList.add(bkaColumns);
            }
        }

        @Override
        public RelNode visit(LogicalFilter filter) {
            super.visit(filter);
            try {
                List<RexNode> conjunctions = RelOptUtil.conjunctions(filter.getCondition());
                for (RexNode node : conjunctions) {
                    if (!RexUtil.containsInputRef(node)) {
                        continue;
                    }
                    if (!(node instanceof RexCall)) {
                        continue;
                    }
                    RexCall call = (RexCall) node;
                    if (call.getOperands().size() != 2) {
                        continue;
                    }
                    RexNode leftRexNode = call.getOperands().get(0);
                    RexNode rightRexNode = call.getOperands().get(1);
                    switch (node.getKind()) {
                    case IN:
                        if (!(rightRexNode instanceof RexCall)) {
                            break;
                        }
                        if (leftRexNode instanceof RexInputRef && rightRexNode
                            .isA(SqlKind.ROW)) {
                            if (DrdsRexFolder.fold(rightRexNode, plannerContext) != null) {
                                addEqualColumnList(filter.getInput(), ImmutableList.of(leftRexNode));
                                break;
                            }
                        }
                        if (leftRexNode.isA(SqlKind.ROW) && leftRexNode instanceof RexCall && rightRexNode.isA(
                            SqlKind.ROW)) {
                            boolean leftRef = ((RexCall) leftRexNode).getOperands().stream()
                                .allMatch(x -> x instanceof RexInputRef);
                            if (leftRef && DrdsRexFolder.fold(rightRexNode, plannerContext) != null) {
                                addEqualColumnList(filter.getInput(), ((RexCall) leftRexNode).getOperands());
                                break;
                            }
                        }
                        break;
                    case EQUALS:
                        // xx = xx
                        if (leftRexNode instanceof RexInputRef && rightRexNode instanceof RexInputRef) {
                            break;
                        }
                        // xx = ?
                        if (leftRexNode instanceof RexInputRef &&
                            DrdsRexFolder.fold(rightRexNode, plannerContext) != null) {
                            addEqualColumnList(filter.getInput(), ImmutableList.of(leftRexNode));
                            break;
                        }
                        // ? = xx
                        if (rightRexNode instanceof RexInputRef &&
                            DrdsRexFolder.fold(leftRexNode, plannerContext) != null) {
                            addEqualColumnList(filter.getInput(), ImmutableList.of(rightRexNode));
                            break;
                        }
                        //(xx,xx)=(?,?)
                        if (leftRexNode.isA(SqlKind.ROW) && leftRexNode instanceof RexCall
                            && rightRexNode.isA(SqlKind.ROW) && rightRexNode instanceof RexCall) {
                            boolean leftRef = ((RexCall) leftRexNode).getOperands().stream()
                                .allMatch(x -> x instanceof RexInputRef);
                            boolean rightRef = ((RexCall) rightRexNode).getOperands().stream()
                                .allMatch(x -> x instanceof RexInputRef);
                            // (xx,xx)=(xx,xx)
                            if (leftRef && rightRef) {
                                break;
                            }
                            if (leftRef && DrdsRexFolder.fold(rightRexNode, plannerContext) != null) {
                                addEqualColumnList(filter.getInput(), ((RexCall) leftRexNode).getOperands());
                                break;
                            }
                            if (rightRef && DrdsRexFolder.fold(leftRexNode, plannerContext) != null) {
                                addEqualColumnList(filter.getInput(), ((RexCall) rightRexNode).getOperands());
                                break;
                            }
                        }
                        break;
                    default:
                        break;
                    }
                }
                return filter;
            } catch (Util.FoundOne find) {
                return filter;
            }
        }

        private void addEqualColumnList(RelNode input, List<RexNode> references) {
            ImmutableList.Builder<String> builder = ImmutableList.builder();
            for (RexNode rexNode : references) {
                if (!(rexNode instanceof RexInputRef)) {
                    return;
                }
                RelColumnOrigin relColumnOrigin =
                    mq.getColumnOrigin(input, ((RexInputRef) rexNode).getIndex());
                if (relColumnOrigin == null) {
                    return;
                }
                builder.add(relColumnOrigin.getColumnName());
            }
            equalColumnsList.add(builder.build());
        }

        RequiredUniqueColumns build() {
            return new RequiredUniqueColumns(equalColumnsList);
        }
    }

    private static class RequiredUniqueColumns {
        // columns list used in 'equal' or 'in'
        private final Map<String, List<String>> equalColumnsMap;
        private int equalColumnSize;

        public RequiredUniqueColumns(List<List<String>> equalColumnsList) {
            this.equalColumnSize = 0;
            this.equalColumnsMap = Maps.newHashMap();
            for (List<String> columns : equalColumnsList) {
                this.equalColumnsMap.put(columns.get(0), columns);
                this.equalColumnSize += columns.size();
            }
        }

        /**
         * check whether required equal columns can cover current index
         *
         * @param index index to be checked, must be unique
         * @return true if required columns can cover index
         */
        private boolean checkCoverage(IndexMeta index) {
            if (index == null) {
                return false;
            }
            if (!index.isUniqueIndex() && !index.isPrimaryKeyIndex()) {
                return false;
            }
            List<ColumnMeta> indexColumns = index.getKeyColumns();
            // all columns of unique key must be covered using equal condition
            if (indexColumns.size() > equalColumnSize) {
                return false;
            }

            int loc = 0;
            while (loc < indexColumns.size()) {
                List<String> equalColumns = equalColumnsMap.getOrDefault((indexColumns.get(loc).getName()), null);
                if (equalColumns == null) {
                    // can't find equal condition on current index column
                    return false;
                }
                loc++;
                for (int i = 1; i < equalColumns.size(); i++) {
                    if (loc == indexColumns.size()) {
                        return true;
                    }
                    if (!equalColumns.get(i).equals(indexColumns.get(loc).getName())) {
                        return false;
                    }
                    loc++;
                }
            }
            return loc == indexColumns.size();
        }
    }

    private static class RequiredColumnsCollector extends RelShuttleImpl {
        // columns list used in 'equal' or 'in'
        private final List<List<String>> equalColumnsList;
        // columns used in order by
        private List<String> orderByColumns;
        private boolean collectSuccess;
        private final RelMetadataQuery mq;
        private final PlannerContext plannerContext;

        public RequiredColumnsCollector(TableScan tableScan, RelMetadataQuery mq, List<String> bkaColumns) {
            this.mq = mq;
            this.plannerContext = PlannerContext.getPlannerContext(tableScan);
            equalColumnsList = Lists.newArrayList();
            orderByColumns = null;
            collectSuccess = true;
            if (CollectionUtils.isNotEmpty(bkaColumns)) {
                equalColumnsList.add(bkaColumns);
            }
        }

        @Override
        public RelNode visit(LogicalFilter filter) {
            super.visit(filter);
            // failed already
            if (!collectSuccess) {
                return filter;
            }
            // consider filters before sort
            if (orderByColumns != null) {
                return filter;
            }
            try {

                List<RexNode> conjunctions = RelOptUtil.conjunctions(filter.getCondition());
                for (RexNode node : conjunctions) {
                    if (!RexUtil.containsInputRef(node)) {
                        continue;
                    }
                    // only consider searchable filters
                    if (!node.getKind().belongsTo(SqlKind.SARGABLE)) {
                        throw Util.FoundOne.NULL;
                    }
                    if (!(node instanceof RexCall)) {
                        throw Util.FoundOne.NULL;
                    }
                    RexCall call = (RexCall) node;
                    switch (node.getKind()) {
                    case IN:
                        if (call.getOperands().size() == 2) {
                            RexNode leftRexNode = call.getOperands().get(0);
                            RexNode rightRexNode = call.getOperands().get(1);
                            if (leftRexNode instanceof RexInputRef && rightRexNode instanceof RexCall && rightRexNode
                                .isA(SqlKind.ROW)) {
                                if (DrdsRexFolder.fold(rightRexNode, plannerContext) != null) {
                                    addEqualColumnList(filter.getInput(), ImmutableList.of(leftRexNode));
                                    break;
                                }
                            }
                            if (leftRexNode.isA(SqlKind.ROW) && leftRexNode instanceof RexCall && rightRexNode.isA(
                                SqlKind.ROW)) {
                                boolean leftRef = ((RexCall) leftRexNode).getOperands().stream()
                                    .allMatch(x -> x instanceof RexInputRef);
                                if (leftRef && DrdsRexFolder.fold(rightRexNode, plannerContext) != null) {
                                    addEqualColumnList(filter.getInput(), ((RexCall) leftRexNode).getOperands());
                                    break;
                                }
                            }
                        }
                        throw Util.FoundOne.NULL;
                    case EQUALS:
                        if (call.getOperands().size() == 2) {
                            RexNode leftRexNode = call.getOperands().get(0);
                            RexNode rightRexNode = call.getOperands().get(1);
                            // xx = xx
                            if (leftRexNode instanceof RexInputRef && rightRexNode instanceof RexInputRef) {
                                break;
                            }
                            // xx = ?
                            if (leftRexNode instanceof RexInputRef &&
                                DrdsRexFolder.fold(rightRexNode, plannerContext) != null) {
                                addEqualColumnList(filter.getInput(), ImmutableList.of(leftRexNode));
                                break;
                            }
                            // ? = xx
                            if (rightRexNode instanceof RexInputRef &&
                                DrdsRexFolder.fold(leftRexNode, plannerContext) != null) {
                                addEqualColumnList(filter.getInput(), ImmutableList.of(rightRexNode));
                                break;
                            }
                            //(xx,xx)=(?,?)
                            if (leftRexNode.isA(SqlKind.ROW) && rightRexNode.isA(SqlKind.ROW)
                                && leftRexNode instanceof RexCall && rightRexNode instanceof RexCall) {
                                boolean leftRef = ((RexCall) leftRexNode).getOperands().stream()
                                    .allMatch(x -> x instanceof RexInputRef);
                                boolean rightRef = ((RexCall) rightRexNode).getOperands().stream()
                                    .allMatch(x -> x instanceof RexInputRef);
                                // (xx,xx)=(xx,xx)
                                if (leftRef && rightRef) {
                                    break;
                                }
                                if (leftRef && DrdsRexFolder.fold(rightRexNode, plannerContext) != null) {
                                    addEqualColumnList(filter.getInput(), ((RexCall) leftRexNode).getOperands());
                                    break;
                                }
                                if (rightRef && DrdsRexFolder.fold(leftRexNode, plannerContext) != null) {
                                    addEqualColumnList(filter.getInput(), ((RexCall) rightRexNode).getOperands());
                                    break;
                                }
                            }
                        }
                        throw Util.FoundOne.NULL;
                    case IS_NULL:
                        RexNode rexNode = call.getOperands().get(0);
                        if (rexNode instanceof RexInputRef) {
                            RelColumnOrigin relColumnOrigin =
                                mq.getColumnOrigin(filter.getInput(), ((RexInputRef) rexNode).getIndex());
                            if (relColumnOrigin != null) {
                                equalColumnsList.add(ImmutableList.of(relColumnOrigin.getColumnName()));
                                break;
                            }
                        }
                        throw Util.FoundOne.NULL;
                    case NOT_EQUALS:
                    case LESS_THAN:
                    case GREATER_THAN:
                    case GREATER_THAN_OR_EQUAL:
                    case LESS_THAN_OR_EQUAL:
                        // reference input columns
                        if (call.getOperands().size() == 2) {
                            RexNode leftRexNode = call.getOperands().get(0);
                            RexNode rightRexNode = call.getOperands().get(1);
                            if (RexUtil.containsInputRef(leftRexNode) && RexUtil.containsInputRef(rightRexNode)) {
                                break;
                            }
                        }
                        throw Util.FoundOne.NULL;
                    case BETWEEN:
                    default:
                        throw Util.FoundOne.NULL;
                    }
                }
                return filter;
            } catch (Util.FoundOne find) {
                collectSuccess = false;
                return filter;
            }
        }

        @Override
        public RelNode visit(LogicalSort sort) {
            super.visit(sort);
            try {
                if (orderByColumns != null) {
                    throw Util.FoundOne.NULL;
                }
                ImmutableList.Builder<String> builder = ImmutableList.builder();
                for (RelFieldCollation relFieldCollation : sort.getCollation().getFieldCollations()) {
                    RelColumnOrigin relColumnOrigin =
                        mq.getColumnOrigin(sort.getInput(), relFieldCollation.getFieldIndex());
                    if (relColumnOrigin == null) {
                        throw Util.FoundOne.NULL;
                    }
                    builder.add(relColumnOrigin.getColumnName());
                }
                orderByColumns = builder.build();
            } catch (Util.FoundOne find) {
                collectSuccess = false;
                return sort;
            }
            return sort;
        }

        private void addEqualColumnList(RelNode input, List<RexNode> references) {
            // todo: check type equal
            ImmutableList.Builder<String> builder = ImmutableList.builder();
            for (RexNode rexNode : references) {
                if (!(rexNode instanceof RexInputRef)) {
                    throw Util.FoundOne.NULL;
                }
                RelColumnOrigin relColumnOrigin = mq.getColumnOrigin(input, ((RexInputRef) rexNode).getIndex());
                if (relColumnOrigin == null) {
                    throw Util.FoundOne.NULL;
                }
                builder.add(relColumnOrigin.getColumnName());
            }
            equalColumnsList.add(builder.build());
        }

        RequiredColumns build() {
            if (!collectSuccess) {
                return null;
            }
            RequiredColumns requiredColumns = new RequiredColumns(equalColumnsList, orderByColumns);
            if (!requiredColumns.build()) {
                return null;
            }
            if (requiredColumns.size() == 0) {
                return null;
            }
            return requiredColumns;
        }
    }

    private static class RequiredColumns {
        // columns list used in 'equal' or 'in'
        private final List<List<String>> equalColumnsList;
        // columns used in order by
        private final List<String> orderByColumns;
        private Map<String, List<String>> equalColumnsMap;
        private int equalColumnSize;
        private int totalColumnSize;

        public RequiredColumns(List<List<String>> equalColumnsList, List<String> orderByColumns) {
            this.equalColumnsList = equalColumnsList;
            this.orderByColumns = orderByColumns;
            equalColumnSize = 0;
            totalColumnSize = 0;
        }

        private boolean build() {
            equalColumnsMap = Maps.newHashMap();
            for (List<String> columns : equalColumnsList) {
                if (equalColumnsMap.put(columns.get(0), columns) != null) {
                    return false;
                }
                equalColumnSize += columns.size();
            }
            if (CollectionUtils.isNotEmpty(orderByColumns)) {
                if (equalColumnsMap.containsKey(orderByColumns.get(0))) {
                    return false;
                }
                totalColumnSize = orderByColumns.size();
            }
            totalColumnSize += equalColumnSize;
            return true;
        }

        /**
         * get equal-condition-column list containing given column.
         *
         * @param column given column
         * @return column set contains given column
         */
        List<String> getEqualColumns(String column) {
            return equalColumnsMap.getOrDefault(column, null);
        }

        public List<String> getOrderByColumns() {
            return orderByColumns;
        }

        int size() {
            return totalColumnSize;
        }

        int equalColumnSize() {
            return equalColumnSize;
        }

        /**
         * check whether current index can cover all required columns
         *
         * @param index index to be checked
         * @return true if index can cover all required columns
         */
        private boolean checkIndexCoverage(IndexMeta index) {
            List<ColumnMeta> indexColumns = index.getKeyColumns();
            // all required columns must be covered
            if (indexColumns.size() < size()) {
                return false;
            }

            int loc = 0;
            // cover equal columns
            while (loc < equalColumnSize()) {
                List<String> equalColumns = getEqualColumns(indexColumns.get(loc).getName());
                if (equalColumns == null) {
                    // can't find equal condition on current index column
                    return false;
                }
                loc++;
                for (int i = 1; i < equalColumns.size(); i++) {
                    if (!equalColumns.get(i).equals(indexColumns.get(loc).getName())) {
                        return false;
                    }
                    loc++;
                }
            }

            List<String> orderByColumns = getOrderByColumns();
            if (CollectionUtils.isEmpty(orderByColumns)) {
                return true;
            }
            // cover order by column
            for (String orderByColumn : orderByColumns) {
                if (!orderByColumn.equals(indexColumns.get(loc).getName())) {
                    return false;
                }
                loc++;
            }
            return true;
        }
    }
}