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

import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.MysqlTableScan;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.view.ViewPlan;
import com.alibaba.polardbx.rule.TableRule;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdColumnUniqueness;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DrdsRelMdColumnUniqueness extends RelMdColumnUniqueness {

    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(BuiltInMethod.COLUMN_UNIQUENESS.method,
            new DrdsRelMdColumnUniqueness());

    public Boolean areColumnsUnique(LogicalView rel, RelMetadataQuery mq, ImmutableBitSet columns,
                                    boolean ignoreNulls) {
        return rel.areColumnsUnique(mq, columns, ignoreNulls);
    }

    public Boolean areColumnsUnique(ViewPlan rel, RelMetadataQuery mq, ImmutableBitSet columns,
                                    boolean ignoreNulls) {
        return mq.areColumnsUnique(rel.getPlan(), columns, ignoreNulls);
    }

    public Boolean areColumnsUnique(MysqlTableScan rel, RelMetadataQuery mq, ImmutableBitSet columns,
                                    boolean ignoreNulls) {
        return mq.areColumnsUnique(rel.getNodeForMetaQuery(), columns, ignoreNulls);
    }

    public Boolean areColumnsUnique(LogicalTableScan rel, RelMetadataQuery mq, ImmutableBitSet columns,
                                    boolean ignoreNulls) {
        TableMeta tableMeta = CBOUtil.getTableMeta(rel.getTable());
        if (tableMeta == null) {
            return null;
        }

        if (DrdsRelMdSelectivity.isPrimaryKeyAutoIncrement(tableMeta) && columns.cardinality() > 0) {
            int pkIndex = DrdsRelMdSelectivity.getColumnIndex(tableMeta,
                tableMeta.getPrimaryIndex().getKeyColumns().get(0));
            if (columns.contains(ImmutableBitSet.of(pkIndex))) {
                return true;
            }
        }

        TddlRuleManager tddlRuleManager =
            PlannerContext.getPlannerContext(rel).getExecutionContext().getSchemaManager(tableMeta.getSchemaName())
                .getTddlRuleManager();
        PartitionInfoManager partitionInfoManager = tddlRuleManager.getPartitionInfoManager();
        List<String> dbKeysAndTbKeys = new ArrayList<>();
        if (partitionInfoManager.isNewPartDbTable(tableMeta.getTableName())) {
            PartitionInfo partitionInfo = partitionInfoManager.getPartitionInfo(tableMeta.getTableName());
            dbKeysAndTbKeys.addAll(partitionInfo.getPartitionColumns());
        } else {
            TableRule tableRule = tddlRuleManager.getTableRule(tableMeta.getTableName());
            if (tddlRuleManager.isShard(tableMeta.getTableName())) {
                dbKeysAndTbKeys.addAll(tableRule.getDbPartitionKeys());
                dbKeysAndTbKeys.addAll(tableRule.getTbPartitionKeys());
            }
        }

        List<IndexMeta> ukList = tableMeta.getUniqueIndexes(true);
        for (IndexMeta uk : ukList) {
            List<String> ukColumns =
                uk.getKeyColumns().stream().map(columnMeta -> columnMeta.getName()).collect(Collectors.toList());
            if (ColumnCoverUkCoverSk(tableMeta, columns, ukColumns, dbKeysAndTbKeys)) {
                return true;
            }
        }

        // gsi
        Map<String, GsiMetaManager.GsiIndexMetaBean> gsiIndexMetaBeanMap = tableMeta.getGsiPublished();
        if (gsiIndexMetaBeanMap != null) {
            for (Map.Entry<String, GsiMetaManager.GsiIndexMetaBean> entry : gsiIndexMetaBeanMap.entrySet()) {
                String gsiName = entry.getKey();
                GsiMetaManager.GsiIndexMetaBean gsiIndexMetaBean = entry.getValue();
                if (!gsiIndexMetaBean.nonUnique) {
                    List<String> ukColumns =
                        gsiIndexMetaBean.indexColumns.stream().map(x -> x.columnName).collect(Collectors.toList());
                    if (ColumnCoverUkCoverSk(tableMeta, columns, ukColumns, new ArrayList<>())) {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    private boolean ColumnCoverUkCoverSk(TableMeta tableMeta, ImmutableBitSet columns, List<String> ukColumns,
                                         List<String> dbKeysAndTbKeys) {
        int[] keyIndexs = new int[ukColumns.size()];
        for (int i = 0; i < ukColumns.size(); i++) {
            keyIndexs[i] = DrdsRelMdSelectivity.getColumnIndex(tableMeta,
                tableMeta.getColumnIgnoreCase(ukColumns.get(i)));
        }
        // uk covered by columns, otherwise bail out
        if (!columns.contains(ImmutableBitSet.of(keyIndexs))) {
            return false;
        }

        /**
         * all dbKeys and tbKeys covered by unique key, example:
         * pk = c1, dbkey = c1, tbkey = c2 return false
         * pk = c1, dbkey = c2, tbkey = c1 return false
         * pk = c1、c2, dbkey = c1, tbkey = c1 return true
         * pk = c1、c2 , dbkey = c1, tbkey = c3 return false
         */

        if (dbKeysAndTbKeys.isEmpty()) {
            return true;
        }
        for (String key : dbKeysAndTbKeys) {
            boolean found = false;
            for (String columnName : ukColumns) {
                if (columnName.equalsIgnoreCase(key)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                return false;
            }
        }
        return true;
    }

    public Boolean areColumnsUnique(RelSubset subset, RelMetadataQuery mq, ImmutableBitSet columns,
                                    boolean ignoreNulls) {
        return mq.areColumnsUnique(Util.first(subset.getBest(), subset.getOriginal()), columns, ignoreNulls);
    }

    public Boolean areColumnsUnique(SemiJoin rel, RelMetadataQuery mq,
                                    ImmutableBitSet columns, boolean ignoreNulls) {
        if (rel.getJoinType() == JoinRelType.LEFT) {
            return null;
        }
        // only return the unique keys from the LHS since a semijoin only
        // returns the LHS
        return mq.areColumnsUnique(rel.getLeft(), columns, ignoreNulls);
    }
}
