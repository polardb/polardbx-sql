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

package com.alibaba.polardbx.optimizer.config.meta;

import com.alibaba.polardbx.common.utils.TreeMaps;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.MysqlTableScan;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.view.ViewPlan;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableLookup;
import org.apache.calcite.rel.metadata.BuiltInMetadata.CoveringIndex;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * @author chenmo.cm
 */
public class DrdsRelMdCoveringIndex implements MetadataHandler<CoveringIndex> {

    public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider
        .reflectiveSource(BuiltInMethod.COVERING_INDEX.method, new DrdsRelMdCoveringIndex());

    @Override
    public MetadataDef<CoveringIndex> getDef() {
        return CoveringIndex.DEF;
    }

    public List<Set<RelColumnOrigin>> isCoveringIndex(Filter rel, RelMetadataQuery mq, RelOptTable table,
                                                      String index) {
        final List<Set<RelColumnOrigin>> origins = mq.isCoveringIndex(rel.getInput(), table, index);

        if (null == origins) {
            return null;
        }

        if (hasPrimaryColumn(table, origins, rel.getCondition())) {
            return null;
        }

        return origins;
    }

    public List<Set<RelColumnOrigin>> isCoveringIndex(ViewPlan rel, RelMetadataQuery mq, RelOptTable table,
                                                      String index) {
        return mq.isCoveringIndex(rel.getPlan(), table, index);
    }

    public List<Set<RelColumnOrigin>> isCoveringIndex(MysqlTableScan rel, RelMetadataQuery mq, RelOptTable table,
                                                      String index) {
        return mq.isCoveringIndex(rel.getNodeForMetaQuery(), table, index);
    }

    public List<Set<RelColumnOrigin>> isCoveringIndex(Sort rel, RelMetadataQuery mq, RelOptTable table, String index) {
        final List<Set<RelColumnOrigin>> origins = mq.isCoveringIndex(rel.getInput(), table, index);

        if (null == origins) {
            return null;
        }

        for (RexNode rexNode : rel.getChildExps()) {
            if (hasPrimaryColumn(table, origins, rexNode)) {
                return null;
            }
        }

        return origins;
    }

    public List<Set<RelColumnOrigin>> isCoveringIndex(Aggregate rel, RelMetadataQuery mq, final RelOptTable table,
                                                      String index) {
        final List<Set<RelColumnOrigin>> origins = mq.isCoveringIndex(rel.getInput(), table, index);

        if (null == origins) {
            return null;
        }

        List<Set<RelColumnOrigin>> result = new ArrayList<>();
        for (int iOutputColumn = 0; iOutputColumn < rel.getRowType().getFieldCount(); iOutputColumn++) {
            if (iOutputColumn < rel.getGroupCount()) {
                final Set<RelColumnOrigin> relColumnOrigins = origins.get(iOutputColumn);
                if (relColumnOrigins.stream().anyMatch(colOrigin -> colOrigin.getOriginTable().equals(table))) {
                    // Referencing column in primary table
                    return null;
                }
                result.add(relColumnOrigins);
                continue;
            }

            if (rel.indicator) {
                if (iOutputColumn < rel.getGroupCount() + rel.getIndicatorCount()) {
                    // The indicator column is originated here.
                    result.add(ImmutableSet.of());
                    continue;
                }
            }

            // Aggregate columns are derived from input columns
            AggregateCall call = rel.getAggCallList()
                .get(iOutputColumn - rel.getGroupCount() - rel.getIndicatorCount());

            final Set<RelColumnOrigin> set = new HashSet<>();
            for (Integer iInput : call.getArgList()) {
                final Set<RelColumnOrigin> inputSet = createDerivedColumnOrigins(origins.get(iInput), table, false);

                if (null == inputSet) {
                    // Referencing column in primary table
                    return null;
                }
                set.addAll(inputSet);
            }
            result.add(set);
        }
        return result;
    }

    public List<Set<RelColumnOrigin>> isCoveringIndex(Project rel, final RelMetadataQuery mq, RelOptTable table,
                                                      String index) {
        final RelNode input = rel.getInput();

        final List<Set<RelColumnOrigin>> origins = mq.isCoveringIndex(input, table, index);

        if (null == origins) {
            return null;
        }

        final List<Set<RelColumnOrigin>> result = new ArrayList<>();
        for (RexNode rexNode : rel.getProjects()) {
            Set<RelColumnOrigin> columnOrigins = null;
            if (rexNode instanceof RexInputRef) {
                // Direct reference: no derivation added, might be pruned later.
                final RexInputRef inputRef = (RexInputRef) rexNode;
                columnOrigins = origins.get(inputRef.getIndex());
            } else {
                // Anything else is a derivation, possibly from multiple columns.
                final AtomicBoolean illegalRef = new AtomicBoolean(false);
                final Set<RelColumnOrigin> set = getColumnReference(origins, rexNode, illegalRef);

                if (illegalRef.get()) {
                    return null;
                }

                // Derived column might be pruned later
                columnOrigins = createDerivedColumnOrigins(set, table, true);
            }

            result.add(columnOrigins);
        }

        return result;
    }

    /**
     * isCoveringIndex for Join means a shard table join with multi broadcast table
     */
    public List<Set<RelColumnOrigin>> isCoveringIndex(Join rel, RelMetadataQuery mq, RelOptTable table, String index) {
        // ignore subquery
        if (rel instanceof SemiJoin) {
            return null;
        }

        final int nLeftColumns = rel.getLeft().getRowType().getFieldList().size();

        final List<Set<RelColumnOrigin>> leftOrigins = mq.isCoveringIndex(rel.getLeft(), table, index);
        final List<Set<RelColumnOrigin>> rightOrigins = mq.isCoveringIndex(rel.getRight(), table, index);

        if (null == leftOrigins || null == rightOrigins) {
            return null;
        }

        List<Set<RelColumnOrigin>> result = new ArrayList<>();
        for (int ci = 0; ci < rel.getRowType().getFieldCount(); ci++) {
            Set<RelColumnOrigin> set;
            if (ci < nLeftColumns) {
                set = leftOrigins.get(ci);
                // null generation does not change column name
            } else {
                set = rightOrigins.get(ci - nLeftColumns);
                // null generation does not change column name
            }
            result.add(set);
        }

        if (hasPrimaryColumn(table, result, rel.getCondition())) {
            return null;
        }

        return result;
    }

    public List<Set<RelColumnOrigin>> isCoveringIndex(SetOp rel, RelMetadataQuery mq, RelOptTable table, String index) {
        return null;
    }

    public List<Set<RelColumnOrigin>> isCoveringIndex(TableLookup rel, final RelMetadataQuery mq, RelOptTable table,
                                                      String index) {
        return null;
    }

    public List<Set<RelColumnOrigin>> isCoveringIndex(Exchange rel, RelMetadataQuery mq, RelOptTable table,
                                                      String index) {
        return null;
    }

    public List<Set<RelColumnOrigin>> isCoveringIndex(TableFunctionScan rel, RelMetadataQuery mq) {
        return null;
    }

    public List<Set<RelColumnOrigin>> isCoveringIndex(LogicalView rel, RelMetadataQuery mq, RelOptTable table,
                                                      String index) {
        return rel.isCoveringIndex(mq, table, index);
    }

    public List<Set<RelColumnOrigin>> isCoveringIndex(RelSubset rel, RelMetadataQuery mq, RelOptTable table,
                                                      String index) {
        return mq.isCoveringIndex(rel.getOriginal(), table, index);
    }

    public List<Set<RelColumnOrigin>> isCoveringIndex(HepRelVertex rel, RelMetadataQuery mq, RelOptTable table,
                                                      String index) {
        return mq.isCoveringIndex(rel.getCurrentRel(), table, index);
    }

    // Catch-all rule when none of the others apply.
    public List<Set<RelColumnOrigin>> isCoveringIndex(RelNode rel, RelMetadataQuery mq, RelOptTable table,
                                                      String index) {
        // NOTE jvs 28-Mar-2006: We may get this wrong for a physical table
        // expression which supports projections. In that case,
        // it's up to the plugin writer to override with the
        // correct information.

        if (rel.getInputs().size() > 0) {
            // No generic logic available for non-leaf rels.
            return null;
        }

        RelOptTable relTable = rel.getTable();
        if (relTable == null) {
            // Somebody is making column values up out of thin air, like a
            // VALUES clause, so we return empty set for each column in row
            // type.
            return null;
        }

        // Detect the case where a physical table expression is performing
        // projection, and say we don't know instead of making any assumptions.
        // (Theoretically we could try to map the projection using column
        // names.) This detection assumes the table expression doesn't handle
        // rename as well.
        if (relTable.getRowType() != rel.getRowType()) {
            return null;
        }

        if (!relTable.equals(table)) {
            // Not primary table we are looking for
            return mq.getColumnOriginNames(rel);
        }

        final Pair<String, String> schemaTable = RelUtils.getQualifiedTableName(relTable);
        if (!GlobalIndexMeta.isPublishedPrimaryAndIndex(schemaTable.right, index, schemaTable.left,
            PlannerContext.getPlannerContext(rel).getExecutionContext())) {
            // Index not belongs to table
            return null;
        }

        // Get index table
        final String schema = Optional.ofNullable(schemaTable.left)
            .orElse(PlannerContext.getPlannerContext(rel).getSchemaName());
        final RelOptSchema catalog =
            RelUtils.buildCatalogReader(schema, PlannerContext.getPlannerContext(rel).getExecutionContext());
        final RelOptTable indexTable = catalog.getTableForMember(ImmutableList.of(schema, index));

        final RelDataType primaryRowType = relTable.getRowType();
        final RelDataType indexRowType = indexTable.getRowType();
        final Map<String, Integer> indexColumnRefMap = indexRowType.getFieldList()
            .stream()
            .collect(Collectors.toMap(RelDataTypeField::getName,
                RelDataTypeField::getIndex,
                (x, y) -> y,
                TreeMaps::caseInsensitiveMap));

        return primaryRowType.getFieldList().stream().map(field -> {
            if (indexColumnRefMap.containsKey(field.getName())) {
                return ImmutableSet.of(new RelColumnOrigin(indexTable, indexColumnRefMap.get(field.getName()), false));
            } else {
                return ImmutableSet.of(new RelColumnOrigin(relTable, field.getIndex(), false));
            }
        }).collect(Collectors.toList());
    }

    private static Set<RelColumnOrigin> createDerivedColumnOrigins(Set<RelColumnOrigin> inputSet, RelOptTable primary,
                                                                   boolean acceptPrimaryRef) {
        final Set<RelColumnOrigin> set = new HashSet<>();
        for (RelColumnOrigin rco : inputSet) {
            if (!acceptPrimaryRef && primary.equals(rco.getOriginTable())) {
                // Referencing column in primary table
                return null;
            }

            RelColumnOrigin derived = new RelColumnOrigin(rco.getOriginTable(), rco.getOriginColumnOrdinal(), true);
            set.add(derived);
        }

        return set;
    }

    private static boolean hasPrimaryColumn(RelOptTable table, List<Set<RelColumnOrigin>> origins, RexNode rexNode) {
        // Referencing column in primary table
        final AtomicBoolean illegalRef = new AtomicBoolean(false);
        final Set<RelColumnOrigin> relColumnOrigins = getColumnReference(origins, rexNode, illegalRef);

        if (illegalRef.get()) {
            return true;
        }

        return relColumnOrigins.stream().anyMatch(colOrigin -> colOrigin.getOriginTable().equals(table));
    }

    private static Set<RelColumnOrigin> getColumnReference(List<Set<RelColumnOrigin>> origins, RexNode rexNode, final
    AtomicBoolean illegalRef) {
        final Set<RelColumnOrigin> set = new HashSet<>();
        final RexVisitor<Void> visitor = new RexVisitorImpl<Void>(true) {

            @Override
            public Void visitInputRef(RexInputRef inputRef) {
                set.addAll(origins.get(inputRef.getIndex()));
                return null;
            }

            @Override
            public Void visitSubQuery(RexSubQuery subQuery) {
                illegalRef.set(true);
                return null;
            }

            @Override
            public Void visitFieldAccess(RexFieldAccess fieldAccess) {
                illegalRef.set(true);
                return null;
            }

            @Override
            public Void visitDynamicParam(RexDynamicParam dynamicParam) {
                final RexUtil.DynamicDeepFinder dynamicDeepFinder = new RexUtil.DynamicDeepFinder(Lists.newArrayList());
                dynamicParam.accept(dynamicDeepFinder);
                if (dynamicDeepFinder.getScalar().size() > 0) {
                    illegalRef.set(true);
                    return null;
                }

                return null;
            }
        };
        rexNode.accept(visitor);

        return set;
    }
}
