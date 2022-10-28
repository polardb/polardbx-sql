/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.optimizer.sharding.result;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import com.alibaba.polardbx.common.DefaultSchema;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStep;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.optimizer.sharding.ConditionExtractor;
import com.alibaba.polardbx.optimizer.sharding.utils.ExtractorContext;
import com.alibaba.polardbx.optimizer.sharding.label.Label;
import com.alibaba.polardbx.optimizer.sharding.result.ExtractionResultVisitor.ResultBean;
import com.alibaba.polardbx.optimizer.utils.RelUtils;

/**
 * @author chenmo.cm
 */
public class ExtractionResult {

    private final ConditionExtractor extractor;
    private final ExtractorContext context;

    private final Map<RelNode, Label> relLabelMap;
    private final Map<RelOptTable, Map<RelNode, Label>> tableLabelMap;

    private ResultBean intersectResultBean = null;

    public ExtractionResult(ConditionExtractor extractor) {
        this.extractor = extractor;
        this.context = extractor.getContext();
        this.relLabelMap = extractor.getRelLabelMap();
        this.tableLabelMap = extractor.getTableLabelMap();
    }

    /**
     * <pre>
     *     key: logical table name
     *     val: the part pruning step for logical table name
     *     Notice: a logical view may has multi logical table when join is pushed.
     * </pre>
     */
    public Map<String, PartitionPruneStep> allPartPruneSteps(ExecutionContext ec) {
        Map<String, PartitionPruneStep> allTblPruneStepInfo = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);

        for (RelOptTable t : getLogicalTables()) {
            ConditionResult condRs = conditionOf(t).intersect();
            List<String> qualifiedName = t.getQualifiedName();
            String schema = qualifiedName.size() > 1 ? qualifiedName.get(qualifiedName.size() - 2) : null;
            if (schema == null) {
                schema = DefaultSchema.getSchemaName();
            }
            final String tableName = Util.last(qualifiedName);
            if (condRs instanceof NormalConditionResult) {
                NormalConditionResult normalCondRs = (NormalConditionResult) condRs;
                PartitionPruneStep stepInfo = normalCondRs.toPartPruneStep(ec);
                if (stepInfo != null) {
                    allTblPruneStepInfo.put(tableName, stepInfo);
                }
            } else if (condRs instanceof EmptyConditionResult) {
                PartitionInfo partInfo =
                    ec.getSchemaManager(schema).getTddlRuleManager().getPartitionInfoManager()
                        .getPartitionInfo(tableName);
                allTblPruneStepInfo
                    .put(tableName, PartitionPruner.generatePartitionPrueStepInfo(partInfo, null, null, ec));
            }
        }
        return allTblPruneStepInfo;

    }

    public void allCondition(Map<String, Map<String, Comparative>> allComps,
                             Map<String, Map<String, Comparative>> allFullComps, ExecutionContext ec) {
        if (null == allComps && null == allFullComps) {
            return;
        }

        for (RelOptTable t : getLogicalTables()) {
            final Map<String, Comparative> comps = conditionOf(t).intersect().toPartitionCondition(ec);
            final Map<String, Comparative> fullComps = conditionOf(t).intersect().toFullPartitionCondition(ec);

            if (null != allComps) {
                allComps.put(Util.last(t.getQualifiedName()), comps);
            }
            if (null != allFullComps) {
                allFullComps.put(Util.last(t.getQualifiedName()), fullComps);
            }
        }
    }

    public PlanShardInfo allShardInfo(ExecutionContext ec) {

        PlanShardInfo planShardInfo = new PlanShardInfo();
        for (RelOptTable relTablle : getLogicalTables()) {
            List<String> qualifiedName = relTablle.getQualifiedName();
            final String tableName = Util.last(qualifiedName);
            String schema = qualifiedName.size() > 1 ? qualifiedName.get(qualifiedName.size() - 2) : null;
            if (schema == null) {
                schema = DefaultSchema.getSchemaName();
            }

            boolean usePartTable = false;
            boolean isNewPartitionDb = DbInfoManager.getInstance().isNewPartitionDb(schema);
            PartitionInfo partInfo = null;
            if (isNewPartitionDb) {
                partInfo =
                    ec.getSchemaManager(schema).getTddlRuleManager().getPartitionInfoManager()
                        .getPartitionInfo(tableName);
                if (partInfo != null) {
                    usePartTable = true;
                }
            }
            RelShardInfo relShardInfo = new RelShardInfo();
            relShardInfo.setUsePartTable(usePartTable);
            relShardInfo.setTableName(tableName);
            relShardInfo.setSchemaName(schema);
            if (!usePartTable) {

                final Map<String, Comparative> comps = conditionOf(relTablle).intersect().toPartitionCondition(ec);
                final Map<String, Comparative> fullComps =
                    conditionOf(relTablle).intersect().toFullPartitionCondition(ec);

                if (null != comps) {
                    relShardInfo.setAllComps(comps);
                }
                if (null != fullComps) {
                    relShardInfo.setAllFullComps(fullComps);
                }
                planShardInfo.putRelShardInfo(schema, tableName, relShardInfo);

            } else {
                ConditionResult condRs = conditionOf(relTablle).intersect();
                if (condRs instanceof NormalConditionResult) {
                    NormalConditionResult normalCondRs = (NormalConditionResult) condRs;
                    PartitionPruneStep stepInfo = normalCondRs.toPartPruneStep(ec);
                    if (stepInfo != null) {
                        relShardInfo.setPartPruneStepInfo(stepInfo);
                        planShardInfo.putRelShardInfo(schema, tableName, relShardInfo);
                    }
                } else if (condRs instanceof EmptyConditionResult) {
                    relShardInfo
                        .setPartPruneStepInfo(PartitionPruner.generatePartitionPrueStepInfo(partInfo, null, null, ec));
                    planShardInfo.putRelShardInfo(schema, tableName, relShardInfo);
                }

            }
        }

        return planShardInfo;
    }

    public void allColumnCondition(Map<String, Map<String, Comparative>> allComps, List<String> columns) {
        if (null == allComps || columns == null) {
            return;
        }

        for (RelOptTable t : getLogicalTables()) {
            final Map<String, Comparative> comps = conditionOf(t).intersect().toColumnCondition(columns);

            if (null != allComps) {
                allComps.put(Util.last(t.getQualifiedName()), comps);
            }
        }
    }

    public ConditionResultSet conditionOf(String schema, String tableName) {
        final String schemaName = schema;

        return tableLabelMap.keySet().stream().filter(k -> {
                final Pair<String, String> qn = RelUtils.getQualifiedTableName(k);
                return TStringUtil.equalsIgnoreCase(qn.left, schemaName)
                    && TStringUtil.equalsIgnoreCase(qn.right, tableName);
            })
            .findAny()
            .map(k -> (ConditionResultSet) new NormalConditionResultSet(this, k))
            .orElse(EmptyConditionResultSet.EMPTY);
    }

    public ConditionResultSet conditionOf(RelOptTable table) {
        if (!tableLabelMap.containsKey(table)) {
            return EmptyConditionResultSet.EMPTY;
        }

        return new NormalConditionResultSet(this, table);
    }

    public ConditionResult conditionOf(RelNode relNode) {
        switch (extractor.getType()) {
        case PARTITIONING_CONDITION:
        case PREDICATE_MOVE_AROUND:
            return ResultFactory.pushdownConditionIn(context, relLabelMap.get(relNode));
        case COLUMN_EQUIVALENCE:
            return ResultFactory.columnEqualityIn(context, relLabelMap.get(relNode));
        default:
            throw new IllegalArgumentException("Unsupported extractor type " + extractor.getType().toString());
        }
    }

    public Label getRootLabel() {
        return extractor.getRootLabel();
    }

    public List<RelOptTable> getLogicalTables() {
        return new ArrayList<>(tableLabelMap.keySet());
    }

    public Set<String> getSchemaNameSet() {
        return tableLabelMap.keySet()
            .stream()
            .map(k -> RelUtils.getQualifiedTableName(k).left)
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());
    }

    ResultBean getIntersectResultBean() {
        if (null == intersectResultBean) {
            final ExtractionResultVisitor visitor = new ExtractionResultVisitor();
            intersectResultBean = Optional.ofNullable(extractor.getRootLabel())
                .map(l -> l.accept(visitor))
                .map(l -> visitor.getResult())
                .orElse(ResultBean.EMPTY);
        }

        return intersectResultBean;
    }

    ExtractorContext getContext() {
        return context;
    }

    public Map<RelNode, Label> getRelLabelMap() {
        return relLabelMap;
    }

    public Map<RelOptTable, Map<RelNode, Label>> getTableLabelMap() {
        return tableLabelMap;
    }
}
