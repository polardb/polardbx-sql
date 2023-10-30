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

package com.alibaba.polardbx.executor.archive.pruning;

import com.alibaba.polardbx.executor.archive.columns.ColumnProvider;
import com.alibaba.polardbx.optimizer.config.table.OSSOrcFileMeta;
import com.alibaba.polardbx.optimizer.config.table.StripeColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import org.apache.orc.DoubleColumnStatistics;
import org.apache.orc.IntegerColumnStatistics;
import org.apache.orc.StringColumnStatistics;
import org.apache.orc.sarg.ExpressionTree;
import org.apache.orc.sarg.PredicateLeaf;
import org.apache.orc.sarg.SearchArgument;

import java.util.List;
import java.util.Map;

/**
 * @author Shi Yuxuan
 */
public class OssAggPruner {
    private OSSOrcFileMeta ossOrcFileMeta;
    private SearchArgument searchArgument;
    private AggPruningResult pruningResult;

    private TableMeta tableMeta;

    public OssAggPruner(OSSOrcFileMeta ossOrcFileMeta, SearchArgument searchArgument,
                        AggPruningResult pruningResult, TableMeta tableMeta) {
        this.ossOrcFileMeta = ossOrcFileMeta;
        this.searchArgument = searchArgument;
        this.pruningResult = pruningResult;
        this.tableMeta = tableMeta;
    }

    public void prune() {
        prune(searchArgument.getExpression());
    }

    private void prune(ExpressionTree expressionTree) {
        if (expressionTree.getOperator() == ExpressionTree.Operator.LEAF) {
            pruneLeaf(searchArgument.getLeaves().get(expressionTree.getLeaf()));
        }
    }

    private void pruneLeaf(PredicateLeaf predicateLeaf) {
        ColumnProvider columnProvider = OssOrcFilePruner.buildColumnProvider(predicateLeaf, tableMeta, ossOrcFileMeta);

        columnProvider.pruneAgg(predicateLeaf, pruningResult.getStripeMap(), this);
    }

    public void addAll(Map<Long, StripeColumnMeta> stripeColumnMetaMap) {
        for (Long key : stripeColumnMetaMap.keySet()) {
            pruningResult.addNotAgg(key);
        }
    }

    public void pruneLong(PredicateLeaf predicateLeaf, Map<Long, StripeColumnMeta> stripeColumnMetaMap) {
        if (predicateLeaf.getOperator() == PredicateLeaf.Operator.BETWEEN) {
            List<Object> literalList = predicateLeaf.getLiteralList();
            long min = ((Number) literalList.get(0)).longValue();
            long max = ((Number) literalList.get(1)).longValue();
            for (Map.Entry<Long, StripeColumnMeta> entry : stripeColumnMetaMap.entrySet()) {
                IntegerColumnStatistics columnStatistics = (IntegerColumnStatistics) entry.getValue()
                    .getColumnStatistics();
                if (min < columnStatistics.getMinimum() && max > columnStatistics.getMaximum()) {
                    continue;
                }
                pruningResult.addNotAgg(entry.getKey());
            }
        } else if (predicateLeaf.getOperator() == PredicateLeaf.Operator.LESS_THAN ||
            predicateLeaf.getOperator() == PredicateLeaf.Operator.LESS_THAN_EQUALS) {
            Object literal = predicateLeaf.getLiteral();
            long max = ((Number) literal).longValue();
            for (Map.Entry<Long, StripeColumnMeta> entry : stripeColumnMetaMap.entrySet()) {
                IntegerColumnStatistics columnStatistics = (IntegerColumnStatistics) entry.getValue()
                    .getColumnStatistics();
                if (max > columnStatistics.getMaximum()) {
                    continue;
                }
                pruningResult.addNotAgg(entry.getKey());
            }
        } else if (predicateLeaf.getOperator() == PredicateLeaf.Operator.GREATER_THAN ||
            predicateLeaf.getOperator() == PredicateLeaf.Operator.GREATER_THAN_EQUALS) {
            Object literal = predicateLeaf.getLiteral();
            long min = ((Number) literal).longValue();
            for (Map.Entry<Long, StripeColumnMeta> entry : stripeColumnMetaMap.entrySet()) {
                IntegerColumnStatistics columnStatistics = (IntegerColumnStatistics) entry.getValue()
                    .getColumnStatistics();
                if (min < columnStatistics.getMinimum()) {
                    continue;
                }
                pruningResult.addNotAgg(entry.getKey());
            }
        }
    }

    public void pruneDouble(PredicateLeaf predicateLeaf, Map<Long, StripeColumnMeta> stripeColumnMetaMap) {
        if (predicateLeaf.getOperator() == PredicateLeaf.Operator.BETWEEN) {
            List<Object> literalList = predicateLeaf.getLiteralList();
            double min = ((Number) literalList.get(0)).doubleValue();
            double max = ((Number) literalList.get(1)).doubleValue();
            for (Map.Entry<Long, StripeColumnMeta> entry : stripeColumnMetaMap.entrySet()) {
                DoubleColumnStatistics columnStatistics = (DoubleColumnStatistics) entry.getValue()
                    .getColumnStatistics();
                if (min < columnStatistics.getMinimum() && max > columnStatistics.getMaximum()) {
                    continue;
                }
                pruningResult.addNotAgg(entry.getKey());
            }
        } else if (predicateLeaf.getOperator() == PredicateLeaf.Operator.LESS_THAN ||
            predicateLeaf.getOperator() == PredicateLeaf.Operator.LESS_THAN_EQUALS) {
            Object literal = predicateLeaf.getLiteral();
            double max = ((Number) literal).doubleValue();
            for (Map.Entry<Long, StripeColumnMeta> entry : stripeColumnMetaMap.entrySet()) {
                DoubleColumnStatistics columnStatistics = (DoubleColumnStatistics) entry.getValue()
                    .getColumnStatistics();
                if (max > columnStatistics.getMaximum()) {
                    continue;
                }
                pruningResult.addNotAgg(entry.getKey());
            }
        } else if (predicateLeaf.getOperator() == PredicateLeaf.Operator.GREATER_THAN ||
            predicateLeaf.getOperator() == PredicateLeaf.Operator.GREATER_THAN_EQUALS) {
            Object literal = predicateLeaf.getLiteral();
            double min = ((Number) literal).doubleValue();
            for (Map.Entry<Long, StripeColumnMeta> entry : stripeColumnMetaMap.entrySet()) {
                DoubleColumnStatistics columnStatistics = (DoubleColumnStatistics) entry.getValue()
                    .getColumnStatistics();
                if (min < columnStatistics.getMinimum()) {
                    continue;
                }
                pruningResult.addNotAgg(entry.getKey());
            }
        }
    }

    public void pruneDecimal(PredicateLeaf predicateLeaf, Map<Long, StripeColumnMeta> stripeColumnMetaMap) {
        // currently, decimal is coded as bytes, so we call pruneBytes to prune partitions
        pruneBytes(predicateLeaf, stripeColumnMetaMap);
    }

    public void pruneBytes(PredicateLeaf predicateLeaf, Map<Long, StripeColumnMeta> stripeColumnMetaMap) {
        if (predicateLeaf.getOperator() == PredicateLeaf.Operator.BETWEEN) {
            List<Object> literalList = predicateLeaf.getLiteralList();
            String min = (String) literalList.get(0);
            String max = (String) literalList.get(1);
            for (Map.Entry<Long, StripeColumnMeta> entry : stripeColumnMetaMap.entrySet()) {
                StringColumnStatistics columnStatistics = (StringColumnStatistics) entry.getValue()
                    .getColumnStatistics();
                if (columnStatistics.getMaximum() == null || columnStatistics.getMaximum() == null) {
                    continue;
                }
                if (min.compareTo(columnStatistics.getMinimum()) < 0
                    && max.compareTo(columnStatistics.getMaximum()) > 0) {
                    continue;
                }
                pruningResult.addNotAgg(entry.getKey());
            }
        } else if (predicateLeaf.getOperator() == PredicateLeaf.Operator.LESS_THAN ||
            predicateLeaf.getOperator() == PredicateLeaf.Operator.LESS_THAN_EQUALS) {
            String max = (String) predicateLeaf.getLiteral();

            for (Map.Entry<Long, StripeColumnMeta> entry : stripeColumnMetaMap.entrySet()) {
                StringColumnStatistics columnStatistics = (StringColumnStatistics) entry.getValue()
                    .getColumnStatistics();
                if (columnStatistics.getMaximum() == null) {
                    continue;
                }
                if (max.compareTo(columnStatistics.getMaximum()) > 0) {
                    continue;
                }
                pruningResult.addNotAgg(entry.getKey());
            }
        } else if (predicateLeaf.getOperator() == PredicateLeaf.Operator.GREATER_THAN ||
            predicateLeaf.getOperator() == PredicateLeaf.Operator.GREATER_THAN_EQUALS) {
            String min = (String) predicateLeaf.getLiteral();
            for (Map.Entry<Long, StripeColumnMeta> entry : stripeColumnMetaMap.entrySet()) {
                StringColumnStatistics columnStatistics = (StringColumnStatistics) entry.getValue()
                    .getColumnStatistics();
                if (columnStatistics.getMinimum() == null) {
                    continue;
                }
                if (min.compareTo(columnStatistics.getMinimum()) < 0) {
                    continue;
                }
                pruningResult.addNotAgg(entry.getKey());
            }
        }
    }
}
