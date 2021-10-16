/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.optimizer.config.meta;

import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.MysqlTableScan;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.GroupJoin;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.apache.commons.lang.ArrayUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * RelMdColumnOrigins supplies a default implementation of
 * {@link RelMetadataQuery#getFunctionalDependency} for the standard logical algebra.
 */
public class DrdsRelMdFunctionalDependency
    implements MetadataHandler<BuiltInMetadata.FunctionalDependency> {
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            BuiltInMethod.FUNCTIONAL_DEPENDENCY.method, new DrdsRelMdFunctionalDependency());

    //~ Constructors -----------------------------------------------------------

    protected DrdsRelMdFunctionalDependency() {
    }

    //~ Methods ----------------------------------------------------------------

    public MetadataDef<BuiltInMetadata.FunctionalDependency> getDef() {
        return BuiltInMetadata.FunctionalDependency.DEF;
    }

    public Map<ImmutableBitSet, ImmutableBitSet> getFunctionalDependency(Aggregate rel,
                                                                         RelMetadataQuery mq,
                                                                         ImmutableBitSet iOutputColumns) {
        Map<ImmutableBitSet, ImmutableBitSet> fd = new LinkedHashMap<>();
        //just keep the group set to through the next pass
        //for gave set 2,4,6,if the Aggregate node is {(0,1,2,3,4),(5,6,)},then the throughSet is 2,4
        final ImmutableBitSet.Builder existsGroupSetBuilder = ImmutableBitSet.builder();
        final ImmutableBitSet.Builder existsGroupSetInputBuilder = ImmutableBitSet.builder();
        final LinkedHashMap<Integer, Integer> mapInToOutPos = new LinkedHashMap<>();
        final ImmutableBitSet groupSet = rel.getGroupSet();
        for (int i = 0, i1 = 0; i < rel.getGroupCount(); i++, i1 += 1) {
            i1 = groupSet.nextSetBit(i1);
            if (iOutputColumns.get(i)) {
                existsGroupSetBuilder.set(i);
                existsGroupSetInputBuilder.set(i1);
                mapInToOutPos.put(i, i1);
            }
        }
        //nesting sub node
        final Collection<Integer> values = mapInToOutPos.values();
        existsGroupSetInputBuilder.addAll(values);
        final Map<ImmutableBitSet, ImmutableBitSet> childUniqueKeyMap =
            mq.getFunctionalDependency(rel.getInput(), existsGroupSetInputBuilder.build());
        //convert to current node FD
        if (childUniqueKeyMap != null && childUniqueKeyMap.size() > 0) {
            for (ImmutableBitSet keySet : childUniqueKeyMap.keySet()) {
                final ImmutableBitSet.Builder keyBuilder = ImmutableBitSet.builder();
                final ImmutableBitSet.Builder valueBuilder = ImmutableBitSet.builder();
                final ImmutableBitSet valuesSet = childUniqueKeyMap.get(keySet);
                for (int i = keySet.nextSetBit(0); i >= 0; i = keySet.nextSetBit(i + 1)) {
                    final Integer integer = mapInToOutPos.get(i);
                    keyBuilder.set(integer);
                }
                for (int i = valuesSet.nextSetBit(0); i >= 0; i = valuesSet.nextSetBit(i + 1)) {
                    final Integer integer = mapInToOutPos.get(i);
                    valueBuilder.set(integer);
                }
                //merge all FD relationship
                fd.put(keyBuilder.build(), valueBuilder.build());
            }
        }
        //find FD from current node
        final ImmutableBitSet existsGroupSet = existsGroupSetBuilder.build();
        final List<Integer> source = new ArrayList(Arrays.asList(ArrayUtils.toObject(iOutputColumns.toArray())));
        final List<Integer> existsGroupSetList =
            new ArrayList(Arrays.asList(ArrayUtils.toObject(existsGroupSet.toArray())));
        //1. throughSetList is not empty
        if (existsGroupSet.cardinality() > 0 && existsGroupSet.contains(groupSet) && !existsGroupSetList
            .containsAll(source)) {
            //exists FD
            //Do FD add
            source.removeAll(existsGroupSetList);
            if (!source.isEmpty()) {
                final ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
                source.stream().forEach(t -> builder.set(t));
                fd.put(existsGroupSet, builder.build());
            }
        }
        return fd;
    }

    public Map<ImmutableBitSet, ImmutableBitSet> getFunctionalDependency(GroupJoin rel,
                                                                         RelMetadataQuery mq,
                                                                         ImmutableBitSet iOutputColumns) {
        Map<ImmutableBitSet, ImmutableBitSet> fd = new LinkedHashMap<>();
        //just keep the group set to through the next pass
        //for gave set 2,4,6,if the Aggregate node is {(0,1,2,3,4),(5,6,)},then the throughSet is 2,4
        final ImmutableBitSet.Builder existsGroupSetBuilder = ImmutableBitSet.builder();
        final ImmutableBitSet.Builder existsGroupSetInputBuilder = ImmutableBitSet.builder();
        final LinkedHashMap<Integer, Integer> mapInToOutPos = new LinkedHashMap<>();
        final ImmutableBitSet groupSet = rel.getGroupSet();
        for (int i = 0, i1 = 0; i < rel.getGroupSet().cardinality(); i++, i1 += 1) {
            i1 = groupSet.nextSetBit(i1);
            if (iOutputColumns.get(i)) {
                existsGroupSetBuilder.set(i);
                existsGroupSetInputBuilder.set(i1);
                mapInToOutPos.put(i, i1);
            }
        }
        //nesting sub node
        final Collection<Integer> values = mapInToOutPos.values();
        existsGroupSetInputBuilder.addAll(values);
        final Map<ImmutableBitSet, ImmutableBitSet> childUniqueKeyMap =
            mq.getFunctionalDependency(rel.copyAsJoin(rel.getTraitSet(), rel.getCondition()),
                existsGroupSetInputBuilder.build());
        //convert to current node FD
        if (childUniqueKeyMap != null && childUniqueKeyMap.size() > 0) {
            for (ImmutableBitSet keySet : childUniqueKeyMap.keySet()) {
                final ImmutableBitSet.Builder keyBuilder = ImmutableBitSet.builder();
                final ImmutableBitSet.Builder valueBuilder = ImmutableBitSet.builder();
                final ImmutableBitSet valuesSet = childUniqueKeyMap.get(keySet);
                for (int i = keySet.nextSetBit(0); i >= 0; i = keySet.nextSetBit(i + 1)) {
                    final Integer integer = mapInToOutPos.get(i);
                    keyBuilder.set(integer);
                }
                for (int i = valuesSet.nextSetBit(0); i >= 0; i = valuesSet.nextSetBit(i + 1)) {
                    final Integer integer = mapInToOutPos.get(i);
                    valueBuilder.set(integer);
                }
                //merge all FD relationship
                fd.put(keyBuilder.build(), valueBuilder.build());
            }
        }
        //find FD from current node
        final ImmutableBitSet existsGroupSet = existsGroupSetBuilder.build();
        final List<Integer> source = new ArrayList(Arrays.asList(ArrayUtils.toObject(iOutputColumns.toArray())));
        final List<Integer> existsGroupSetList =
            new ArrayList(Arrays.asList(ArrayUtils.toObject(existsGroupSet.toArray())));
        //1. throughSetList is not empty
        if (existsGroupSet.cardinality() > 0 && existsGroupSet.contains(groupSet) && !existsGroupSetList
            .containsAll(source)) {
            //exists FD
            //Do FD add
            source.removeAll(existsGroupSetList);
            if (!source.isEmpty()) {
                final ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
                source.stream().forEach(t -> builder.set(t));
                fd.put(existsGroupSet, builder.build());
            }
        }
        return fd;
    }

    public Map<ImmutableBitSet, ImmutableBitSet> getFunctionalDependency(Join rel, RelMetadataQuery mq,
                                                                         ImmutableBitSet iOutputColumns) {
        Map<ImmutableBitSet, ImmutableBitSet> fd = new LinkedHashMap<>();
        int nLeftColumns = rel.getLeft().getRowType().getFieldList().size();
        final ImmutableBitSet.Builder leftSet = ImmutableBitSet.builder();
        final ImmutableBitSet.Builder rightSet = ImmutableBitSet.builder();
        for (int i = 0, j = 0; i < iOutputColumns.cardinality(); i++, j += 1) {
            j = iOutputColumns.nextSetBit(j);
            if (j < nLeftColumns) {
                leftSet.set(j);
            } else {
                rightSet.set(j - nLeftColumns);
            }
        }
        //nesting sub node
        if (rel.getJoinType() != JoinRelType.RIGHT) {
            final ImmutableBitSet build = leftSet.build();
            if (build.cardinality() > 0) {
                final Map<ImmutableBitSet, ImmutableBitSet> functionalDependencyLeft =
                    mq.getFunctionalDependency(rel.getLeft(), build);
                if (functionalDependencyLeft != null && functionalDependencyLeft.size() > 0) {
                    fd.putAll(functionalDependencyLeft);
                }
            }
        }

        //nesting sub node
        if (rel.getJoinType() != JoinRelType.LEFT) {
            final ImmutableBitSet build1 = rightSet.build();
            if (build1.cardinality() > 0) {
                final Map<ImmutableBitSet, ImmutableBitSet> functionalDependencyRight =
                    mq.getFunctionalDependency(rel.getRight(), build1);
                if (functionalDependencyRight != null && functionalDependencyRight.size() > 0) {
                    for (ImmutableBitSet keySet : functionalDependencyRight.keySet()) {
                        final ImmutableBitSet.Builder keyBuilder = ImmutableBitSet.builder();
                        final ImmutableBitSet.Builder valueBuilder = ImmutableBitSet.builder();
                        final ImmutableBitSet valuesSet = functionalDependencyRight.get(keySet);
                        for (int i = keySet.nextSetBit(0); i >= 0; i = keySet.nextSetBit(i + 1)) {
                            final Integer integer = nLeftColumns + i;
                            keyBuilder.set(integer);
                        }
                        for (int i = valuesSet.nextSetBit(0); i >= 0; i = valuesSet.nextSetBit(i + 1)) {
                            final Integer integer = nLeftColumns + i;
                            valueBuilder.set(integer);
                        }
                        //merge all FD relationship
                        fd.put(keyBuilder.build(), valueBuilder.build());
                    }
                }
            }
        }
        //if need, we can add the foreign key at here,too do
        //find FD from current node
        return fd;
    }

    public Map<ImmutableBitSet, ImmutableBitSet> getFunctionalDependency(SetOp rel,
                                                                         RelMetadataQuery mq,
                                                                         ImmutableBitSet iOutputColumns) {
        Map<ImmutableBitSet, ImmutableBitSet> fd = new LinkedHashMap<>();
        return fd;
    }

    public Map<ImmutableBitSet, ImmutableBitSet> getFunctionalDependency(LogicalView rel, RelMetadataQuery mq,
                                                                         ImmutableBitSet iOutputColumns) {
        return rel.getFunctionalDependency(mq, iOutputColumns);
    }

    public Map<ImmutableBitSet, ImmutableBitSet> getFunctionalDependency(MysqlTableScan rel, RelMetadataQuery mq,
                                                                         ImmutableBitSet iOutputColumns) {
        Map<ImmutableBitSet, ImmutableBitSet> fd = new LinkedHashMap<>();
        return mq.getFunctionalDependency(rel.getNodeForMetaQuery(), iOutputColumns);
    }

    public Map<ImmutableBitSet, ImmutableBitSet> getFunctionalDependency(Project rel,
                                                                         final RelMetadataQuery mq,
                                                                         ImmutableBitSet iOutputColumns) {
        Map<ImmutableBitSet, ImmutableBitSet> fd = new LinkedHashMap<>();
        final ImmutableBitSet.Builder inputSet = ImmutableBitSet.builder();
        final LinkedHashMap<Integer, Integer> mapInToOutPos = new LinkedHashMap<>();
        for (int i = 0, j = 0; i < iOutputColumns.cardinality(); i++, j++) {
            j = iOutputColumns.nextSetBit(j);
            RexNode rexNode = rel.getProjects().get(j);
            if (rexNode instanceof RexInputRef) {
                final int index = ((RexInputRef) rexNode).getIndex();
                inputSet.set(index);
                mapInToOutPos.put(index, j);
            }
        }
        final ImmutableBitSet inputBitSet = inputSet.build();
        final Map<ImmutableBitSet, ImmutableBitSet> functionalDependency =
            mq.getFunctionalDependency(rel.getInput(), inputBitSet);
        if (functionalDependency != null || functionalDependency.size() > 0) {
            for (ImmutableBitSet keySet : functionalDependency.keySet()) {
                final ImmutableBitSet.Builder keyBuilder = ImmutableBitSet.builder();
                final ImmutableBitSet.Builder valueBuilder = ImmutableBitSet.builder();
                final ImmutableBitSet valuesSet = functionalDependency.get(keySet);
                for (int i = keySet.nextSetBit(0); i >= 0; i = keySet.nextSetBit(i + 1)) {
                    final Integer integer = mapInToOutPos.get(i);
                    keyBuilder.set(integer);
                }
                for (int i = valuesSet.nextSetBit(0); i >= 0; i = valuesSet.nextSetBit(i + 1)) {
                    final Integer integer = mapInToOutPos.get(i);
                    valueBuilder.set(integer);
                }
                //merge all FD relationship
                fd.put(keyBuilder.build(), valueBuilder.build());
            }
        }
        return fd;
    }

    public Map<ImmutableBitSet, ImmutableBitSet> getFunctionalDependency(Filter rel,
                                                                         RelMetadataQuery mq,
                                                                         ImmutableBitSet iOutputColumns) {
        return mq.getFunctionalDependency(rel.getInput(), iOutputColumns);
    }

    public Map<ImmutableBitSet, ImmutableBitSet> getFunctionalDependency(Sort rel, RelMetadataQuery mq,
                                                                         ImmutableBitSet iOutputColumns) {
        Map<ImmutableBitSet, ImmutableBitSet> fd = new LinkedHashMap<>();
        return mq.getFunctionalDependency(rel.getInput(), iOutputColumns);
    }

    public Map<ImmutableBitSet, ImmutableBitSet> getFunctionalDependency(Exchange rel,
                                                                         RelMetadataQuery mq,
                                                                         ImmutableBitSet iOutputColumns) {
        return mq.getFunctionalDependency(rel.getInput(), iOutputColumns);
    }

    public Map<ImmutableBitSet, ImmutableBitSet> getFunctionalDependency(TableScan rel, RelMetadataQuery mq,
                                                                         ImmutableBitSet iOutputColumns) {
        Map<ImmutableBitSet, ImmutableBitSet> fd = new LinkedHashMap<>();
        TableMeta tableMeta = CBOUtil.getTableMeta(rel.getTable());
        if (tableMeta == null) {
            return fd;
        }
        final List<List<String>> keyLists = initKeyList(rel.getTable());
        for (List<String> keyList : keyLists) {
            final List<ColumnMeta> allColumns = tableMeta.getAllColumns();
            final List<Integer> collect = new ArrayList<>();
            for (int i = 0; i < allColumns.size(); i++) {
                final ColumnMeta columnMeta = allColumns.get(i);
                if (keyList != null && keyList.contains(columnMeta.getName().toLowerCase())) {
                    collect.add(i);
                }
            }

            List<Integer> outputKeyList = new ArrayList();
            for (int i = 0, j = 0; i < iOutputColumns.cardinality(); i++, j += 1) {
                j = iOutputColumns.nextSetBit(j);
                if (collect.contains(j)) {
                    outputKeyList.add(j);
                }
            }
            if (outputKeyList.containsAll(collect) && iOutputColumns.cardinality() > outputKeyList.size()) {
                final List<Integer> integers = iOutputColumns.toList();
                integers.removeAll(outputKeyList);
                fd.put(ImmutableBitSet.builder().addAll(outputKeyList).build(),
                    ImmutableBitSet.builder().addAll(integers).build());
            }
        }
        return fd;
    }

    // Catch-all rule when none of the others apply.
    public Map<ImmutableBitSet, ImmutableBitSet> getFunctionalDependency(RelNode rel,
                                                                         RelMetadataQuery mq,
                                                                         ImmutableBitSet iOutputColumns) {
        Map<ImmutableBitSet, ImmutableBitSet> fd = new LinkedHashMap<>();
        return fd;
    }

    private List<List<String>> initKeyList(RelOptTable originTable) {
        List<List<String>> keyLists = new ArrayList<>(8);
        final List<String> qualifiedName = originTable.getQualifiedName();
        String innerSchemaName = qualifiedName.get(0);
        String tableName = Util.last(qualifiedName);
        OptimizerContext optimizerContext = OptimizerContext.getContext(innerSchemaName);
        if (optimizerContext == null) {
            return keyLists;
        }
        final TddlRuleManager rule = optimizerContext.getRuleManager();
        if (rule == null) {
            return keyLists;
        }

        // Get sharding keys
        List<String> shards = rule.getSharedColumns(tableName);
        if (shards != null && shards.size() > 0) {
            //sharding keys to lower
            shards = shards.stream().map(String::toLowerCase).collect(Collectors.toList());
        }

        if (originTable == null) {
            return keyLists;
        }
        if (originTable instanceof RelOptTableImpl) {
            //primary keys
            final Table implTable = ((RelOptTableImpl) originTable).getImplTable();
            if (implTable == null) {
                return keyLists;
            }
            if (implTable instanceof TableMeta) {
                final TableMeta tableMeta = (TableMeta) implTable;
                if (tableMeta == null) {
                    return keyLists;
                }
                // Find PK
                final Collection<ColumnMeta> primaryKey = tableMeta.getPrimaryKey();
                List pkList = primaryKey.stream().map(e -> e.getName()).collect(Collectors.toList());
                if (isFunctionalDependencyKey(pkList, shards)) {
                    keyLists.add(pkList);
                }
                final List<IndexMeta> secondaryIndexes = tableMeta.getSecondaryIndexes();
                // Find UK
                if (secondaryIndexes != null) {
                    for (int i = 0; i < secondaryIndexes.size(); i++) {
                        final IndexMeta indexMeta = secondaryIndexes.get(i);
                        //make sure is a UK
                        if (indexMeta != null && indexMeta.isUniqueIndex() && !indexMeta.isPrimaryKeyIndex()) {
                            List<String> uniqueIndexs = new ArrayList<>();
                            final List<ColumnMeta> keyColumnMetas = indexMeta.getKeyColumns();
                            if (keyColumnMetas == null) {
                                continue;
                            }
                            for (ColumnMeta cm : keyColumnMetas) {
                                if (cm != null) {
                                    uniqueIndexs.add(cm.getName().toLowerCase());
                                }
                            }
                            //is a FD
                            if (isFunctionalDependencyKey(pkList, shards)) {
                                keyLists.add(uniqueIndexs);
                            }
                        }

                    }
                }
            }

        }

        return keyLists;
    }

    private boolean isFunctionalDependencyKey(List<String> keyList, List<String> sharding) {
        //if sharding keys is empty, or sharding key is null, it is means that the table is a single Group table
        if (sharding == null || sharding.isEmpty()) {
            return true;
        }

        //make sure equals
        if (keyList != null && keyList.containsAll(sharding) && sharding.containsAll(keyList)) {
            return true;
        }
        return false;
    }

}

// End RelMdColumnOrigins.java
