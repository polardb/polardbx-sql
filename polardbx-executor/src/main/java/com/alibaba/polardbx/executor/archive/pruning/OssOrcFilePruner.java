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

import com.alibaba.polardbx.common.charset.MySQLUnicodeUtils;
import com.alibaba.polardbx.common.datatype.DecimalConverter;
import com.alibaba.polardbx.common.datatype.DecimalStructure;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.archive.columns.ColumnProvider;
import com.alibaba.polardbx.executor.archive.columns.ColumnProviders;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.OSSOrcFileMeta;
import com.alibaba.polardbx.optimizer.config.table.StripeColumnMeta;
import org.apache.orc.ColumnStatistics;
import org.apache.orc.DoubleColumnStatistics;
import org.apache.orc.IntegerColumnStatistics;
import org.apache.orc.StringColumnStatistics;
import org.apache.orc.sarg.ExpressionTree;
import org.apache.orc.sarg.PredicateLeaf;
import org.apache.orc.sarg.SearchArgument;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author chenzilin
 */
public class OssOrcFilePruner {
    private static final Logger LOGGER = LoggerFactory.getLogger(OssOrcFilePruner.class);
    private OSSOrcFileMeta ossOrcFileMeta;
    private SearchArgument searchArgument;
    private Set<String> filterSet;
    private Pattern pattern;
    private Long readTs;

    public OssOrcFilePruner(OSSOrcFileMeta ossOrcFileMeta, SearchArgument searchArgument,
                            Set<String> filterSet, Pattern pattern, Long readTs) {
        this.ossOrcFileMeta = ossOrcFileMeta;
        this.searchArgument = searchArgument;
        this.filterSet = filterSet;
        this.pattern = pattern;
        this.readTs = readTs;
    }

    public PruningResult prune() {
        String fileName = ossOrcFileMeta.getFileName();

        if (ossOrcFileMeta.getCommitTs() == null) {
            // commitTs == null means file was not committed
            return PruningResult.SKIP;
        }

        if (readTs == null) {
            // removeTs != null means file was dropped
            if (ossOrcFileMeta.getRemoveTs() != null) {
                return PruningResult.SKIP;
            }
        } else {
            // visibility : readTs ∈ [commitTs --- removeTs]
            if (readTs < ossOrcFileMeta.getCommitTs()
                    || (ossOrcFileMeta.getRemoveTs() != null && readTs > ossOrcFileMeta.getRemoveTs())) {
                return PruningResult.SKIP;
            }
        }

        if (filterSet != null && !filterSet.contains(fileName)) {
            return PruningResult.SKIP;
        }

        if (pattern != null && !pattern.matcher(fileName).matches()) {
            return PruningResult.SKIP;
        }

        return prune(searchArgument.getExpression());
    }

    public PruningResult prune(ExpressionTree expressionTree) {
        if (expressionTree.getOperator() == ExpressionTree.Operator.OR) {
            return pruneOr(expressionTree);
        } else if (expressionTree.getOperator() == ExpressionTree.Operator.AND) {
            return pruneAnd(expressionTree);
        } else if (expressionTree.getOperator() == ExpressionTree.Operator.LEAF) {
            return pruneLeaf(searchArgument.getLeaves().get(expressionTree.getLeaf()));
        } else {
            return PruningResult.PASS;
        }
    }

    private PruningResult pruneLeaf(PredicateLeaf predicateLeaf) {
        ColumnMeta columnMeta = ossOrcFileMeta.getColumnMetaMap().get(predicateLeaf.getColumnName());

        ColumnProvider columnProvider = ColumnProviders.getProvider(columnMeta);

        ColumnStatistics columnStatistics = ossOrcFileMeta.getStatisticsMap().get(predicateLeaf.getColumnName());

        Map<Long, StripeColumnMeta> stripeColumnMetaMap =
            (ossOrcFileMeta).getStripeColumnMetas(predicateLeaf.getColumnName());

        PruningResult pruningResult = columnProvider.prune(predicateLeaf, columnStatistics, stripeColumnMetaMap);

        if (pruningResult == PruningResult.SKIP) {
            LOGGER.info("pruning " + ossOrcFileMeta.getFileName() + " with " + predicateLeaf);
        }

        return pruningResult;
    }

    private PruningResult pruneAnd(ExpressionTree expressionTree) {
        assert expressionTree.getOperator() == ExpressionTree.Operator.AND;
        PruningResult andPruningResult = PruningResult.PASS;
        for (ExpressionTree child : expressionTree.getChildren()) {
            PruningResult pruningResult = prune(child);
            if (pruningResult.skip()) {
                // fast path
                return pruningResult;
            } else {
                andPruningResult = andPruningResult.intersect(pruningResult);
            }
        }
        return andPruningResult;
    }

    private PruningResult pruneOr(ExpressionTree expressionTree) {
        assert expressionTree.getOperator() == ExpressionTree.Operator.OR;
        PruningResult orPruningResult = PruningResult.SKIP;
        for (ExpressionTree child : expressionTree.getChildren()) {
            PruningResult pruningResult = prune(child);
            if (pruningResult.pass()) {
                // fast path
                return pruningResult;
            } else {
                orPruningResult = orPruningResult.union(pruningResult);
            }
        }
        return orPruningResult;
    }

    public static PruningResult pruneBytes(PredicateLeaf predicateLeaf, ColumnStatistics columnStatistics,
                                           Map<Long, StripeColumnMeta> stripeColumnMetaMap) {
        StringColumnStatistics stringColumnStatistics = ((StringColumnStatistics) columnStatistics);
        String statisticsMinimum = stringColumnStatistics.getMinimum();
        String statisticsMaximum = stringColumnStatistics.getMaximum();

        if (statisticsMinimum == null || statisticsMaximum == null) {
            return PruningResult.SKIP;
        }

        final PredicateLeaf.Operator operator = predicateLeaf.getOperator();
        switch (operator) {
        case EQUALS: {
            String utf8Str = (String) predicateLeaf.getLiteral();

            if (utf8Str.compareTo(statisticsMaximum) > 0 || utf8Str.compareTo(statisticsMinimum) < 0) {
                return PruningResult.SKIP;
            }

            byte[] utf8Bytes = utf8Str.getBytes();
            List<StripeColumnMeta> stripeColumnMetaList = stripeColumnMetaMap.values().stream().filter(x -> {
                StringColumnStatistics statistics = (StringColumnStatistics) x.getColumnStatistics();
                if (statistics.getMinimum() == null || statistics.getMaximum() == null) {
                    return false;
                }
                return !(utf8Str.compareTo(statistics.getMaximum()) > 0
                    || utf8Str.compareTo(statistics.getMinimum()) < 0);
            }).filter(x -> x.getBloomFilter() == null ? true : x.getBloomFilter().testBytes(utf8Bytes, 0, utf8Bytes.length)).collect(Collectors.toList());
            return generatePruningResult(stripeColumnMetaList, stripeColumnMetaMap);
        }
        case IN: {
            List<String> literalList = predicateLeaf.getLiteralList().stream()
                .map(String.class::cast).collect(Collectors.toList());

            List<StripeColumnMeta> stripeColumnMetaList = stripeColumnMetaMap.values().stream().filter(
                x -> {
                    StringColumnStatistics statistics = (StringColumnStatistics) x.getColumnStatistics();
                    if (statistics.getMinimum() == null || statistics.getMaximum() == null) {
                        return false;
                    }
                    for (String utf8Str : literalList) {
                        if (!(utf8Str.compareTo(statistics.getMaximum()) > 0
                            || utf8Str.compareTo(statistics.getMinimum()) < 0)) {
                            return true;
                        }
                    }
                    return false;
                }
            ).filter(
                x -> {
                    if (x.getBloomFilter() == null) {
                        return false;
                    }
                    for (String literal : literalList) {
                        byte[] utf8Bytes = literal.getBytes();
                        boolean test = x.getBloomFilter().testBytes(utf8Bytes, 0, utf8Bytes.length);
                        if (test) {
                            return true;
                        }
                    }
                    return false;
                }
            ).collect(Collectors.toList());
            return generatePruningResult(stripeColumnMetaList, stripeColumnMetaMap);
        }
        case IS_NULL: {
            boolean test = columnStatistics.hasNull();
            if (!test) {
                return PruningResult.SKIP;
            } else {
                List<StripeColumnMeta> stripeColumnMetaList =
                    stripeColumnMetaMap.values().stream()
                        .filter(x -> x.getColumnStatistics().hasNull()).collect(Collectors.toList());
                return generatePruningResult(stripeColumnMetaList, stripeColumnMetaMap);
            }
        }
        case BETWEEN: {
            List<String> literalList = predicateLeaf.getLiteralList().stream()
                .map(String.class::cast).collect(Collectors.toList());

            String min = literalList.get(0);
            String max = literalList.get(1);

            if (max.compareTo(min) < 0) {
                return PruningResult.SKIP;
            }

            if (min.compareTo(statisticsMaximum) > 0 || max.compareTo(statisticsMinimum) < 0) {
                return PruningResult.SKIP;
            } else {
                List<StripeColumnMeta> stripeColumnMetaList = stripeColumnMetaMap.values().stream().filter(x -> {
                    StringColumnStatistics statistics = (StringColumnStatistics) x.getColumnStatistics();
                    if (statistics.getMinimum() == null || statistics.getMaximum() == null) {
                        return false;
                    }
                    return !(min.compareTo(statistics.getMaximum()) > 0 || max.compareTo(statistics.getMinimum()) < 0);
                }).collect(Collectors.toList());
                return generatePruningResult(stripeColumnMetaList, stripeColumnMetaMap);
            }
        }
        case LESS_THAN: {
            String value = (String) predicateLeaf.getLiteral();

            if (value.compareTo(statisticsMinimum) <= 0) {
                return PruningResult.SKIP;
            } else {
                List<StripeColumnMeta> stripeColumnMetaList = stripeColumnMetaMap.values().stream().filter(x -> {
                    StringColumnStatistics statistics = (StringColumnStatistics) x.getColumnStatistics();
                    if (statistics.getMinimum() == null || statistics.getMaximum() == null) {
                        return false;
                    }
                    return !(value.compareTo(statistics.getMinimum()) <= 0);
                }).collect(Collectors.toList());
                return generatePruningResult(stripeColumnMetaList, stripeColumnMetaMap);
            }
        }
        case LESS_THAN_EQUALS: {
            String value = (String) predicateLeaf.getLiteral();

            if (value.compareTo(statisticsMinimum) < 0) {
                return PruningResult.SKIP;
            } else {
                List<StripeColumnMeta> stripeColumnMetaList = stripeColumnMetaMap.values().stream().filter(x -> {
                    StringColumnStatistics statistics = (StringColumnStatistics) x.getColumnStatistics();
                    if (statistics.getMinimum() == null || statistics.getMaximum() == null) {
                        return false;
                    }
                    return !(value.compareTo(statistics.getMinimum()) < 0);
                }).collect(Collectors.toList());
                return generatePruningResult(stripeColumnMetaList, stripeColumnMetaMap);
            }
        }
        case GREATER_THAN: {
            String value = (String) predicateLeaf.getLiteral();

            if (value.compareTo(statisticsMaximum) >= 0) {
                return PruningResult.SKIP;
            } else {
                List<StripeColumnMeta> stripeColumnMetaList = stripeColumnMetaMap.values().stream().filter(x -> {
                    StringColumnStatistics statistics = (StringColumnStatistics) x.getColumnStatistics();
                    if (statistics.getMinimum() == null || statistics.getMaximum() == null) {
                        return false;
                    }
                    return !(value.compareTo(statistics.getMaximum()) >= 0);
                }).collect(Collectors.toList());
                return generatePruningResult(stripeColumnMetaList, stripeColumnMetaMap);
            }
        }
        case GREATER_THAN_EQUALS: {
            String value = (String) predicateLeaf.getLiteral();

            if (value.compareTo(statisticsMaximum) > 0) {
                return PruningResult.SKIP;
            } else {
                List<StripeColumnMeta> stripeColumnMetaList = stripeColumnMetaMap.values().stream().filter(x -> {
                    StringColumnStatistics statistics = (StringColumnStatistics) x.getColumnStatistics();
                    if (statistics.getMinimum() == null || statistics.getMaximum() == null) {
                        return false;
                    }
                    return !(value.compareTo(statistics.getMaximum()) > 0);
                }).collect(Collectors.toList());
                return generatePruningResult(stripeColumnMetaList, stripeColumnMetaMap);
            }
        }
        default: {
            return PruningResult.PASS;
        }
        }
    }

    public static PruningResult pruneDouble(PredicateLeaf predicateLeaf, ColumnStatistics columnStatistics,
                                            Map<Long, StripeColumnMeta> stripeColumnMetaMap) {
        DoubleColumnStatistics doubleColumnStatistics = ((DoubleColumnStatistics) columnStatistics);
        double statisticsMinimum = doubleColumnStatistics.getMinimum();
        double statisticsMaximum = doubleColumnStatistics.getMaximum();
        if (predicateLeaf.getOperator() == PredicateLeaf.Operator.EQUALS) {
            Object literal = predicateLeaf.getLiteral();

            double value = ((Number) literal).doubleValue();

            if (value > statisticsMaximum || value < statisticsMinimum) {
                return PruningResult.SKIP;
            }

            List<StripeColumnMeta> stripeColumnMetaList = stripeColumnMetaMap.values().stream().filter(x -> {
                DoubleColumnStatistics statistics = (DoubleColumnStatistics) x.getColumnStatistics();
                return !(value > statistics.getMaximum() || value < statistics.getMinimum());
            }).filter(x -> x.getBloomFilter() == null ? true : x.getBloomFilter().testDouble(value)).collect(Collectors.toList());
            return generatePruningResult(stripeColumnMetaList, stripeColumnMetaMap);
        } else if (predicateLeaf.getOperator() == PredicateLeaf.Operator.IN) {
            List<Object> literalList = predicateLeaf.getLiteralList();

            List<StripeColumnMeta> stripeColumnMetaList = stripeColumnMetaMap.values().stream().filter(
                x -> {
                    DoubleColumnStatistics statistics = (DoubleColumnStatistics) x.getColumnStatistics();

                    for (Object literal : literalList) {
                        double value = ((Number) literal).doubleValue();
                        if (!(value > statistics.getMaximum() || value < statistics.getMinimum())) {
                            return true;
                        }
                    }
                    return false;
                }
            ).filter(
                x -> {
                    if (x.getBloomFilter() == null) {
                        return true;
                    }
                    for (Object literal : literalList) {
                        boolean test = x.getBloomFilter().testDouble(((Number) literal).doubleValue());
                        if (test) {
                            return true;
                        }
                    }
                    return false;
                }
            ).collect(Collectors.toList());
            return generatePruningResult(stripeColumnMetaList, stripeColumnMetaMap);
        } else if (predicateLeaf.getOperator() == PredicateLeaf.Operator.IS_NULL) {
            boolean test = columnStatistics.hasNull();
            if (!test) {
                return PruningResult.SKIP;
            } else {
                List<StripeColumnMeta> stripeColumnMetaList =
                    stripeColumnMetaMap.values().stream()
                        .filter(x -> x.getColumnStatistics().hasNull()).collect(Collectors.toList());
                return generatePruningResult(stripeColumnMetaList, stripeColumnMetaMap);
            }
        } else if (predicateLeaf.getOperator() == PredicateLeaf.Operator.BETWEEN) {
            List<Object> literalList = predicateLeaf.getLiteralList();
            double min = ((Number) literalList.get(0)).doubleValue();
            double max = ((Number) literalList.get(1)).doubleValue();

            if (max < min) {
                return PruningResult.SKIP;
            }

            if (min > statisticsMaximum || max < statisticsMinimum) {
                return PruningResult.SKIP;
            } else {
                List<StripeColumnMeta> stripeColumnMetaList = stripeColumnMetaMap.values().stream().filter(x -> {
                    DoubleColumnStatistics statistics = (DoubleColumnStatistics) x.getColumnStatistics();
                    return !(min > statistics.getMaximum() || max < statistics.getMinimum());
                }).collect(Collectors.toList());
                return generatePruningResult(stripeColumnMetaList, stripeColumnMetaMap);
            }
        } else if (predicateLeaf.getOperator() == PredicateLeaf.Operator.LESS_THAN) {
            Object literal = predicateLeaf.getLiteral();

            double value = ((Number) literal).doubleValue();

            if (value <= statisticsMinimum) {
                return PruningResult.SKIP;
            } else {
                List<StripeColumnMeta> stripeColumnMetaList = stripeColumnMetaMap.values().stream().filter(x -> {
                    DoubleColumnStatistics statistics = (DoubleColumnStatistics) x.getColumnStatistics();
                    return !(value <= statistics.getMinimum());
                }).collect(Collectors.toList());
                return generatePruningResult(stripeColumnMetaList, stripeColumnMetaMap);
            }
        } else if (predicateLeaf.getOperator() == PredicateLeaf.Operator.LESS_THAN_EQUALS) {
            Object literal = predicateLeaf.getLiteral();

            double value = ((Number) literal).doubleValue();

            if (value < statisticsMinimum) {
                return PruningResult.SKIP;
            } else {
                List<StripeColumnMeta> stripeColumnMetaList = stripeColumnMetaMap.values().stream().filter(x -> {
                    DoubleColumnStatistics statistics = (DoubleColumnStatistics) x.getColumnStatistics();
                    return !(value < statistics.getMinimum());
                }).collect(Collectors.toList());
                return generatePruningResult(stripeColumnMetaList, stripeColumnMetaMap);
            }
        } else if (predicateLeaf.getOperator() == PredicateLeaf.Operator.GREATER_THAN) {
            Object literal = predicateLeaf.getLiteral();

            double value = ((Number) literal).doubleValue();

            if (value >= statisticsMaximum) {
                return PruningResult.SKIP;
            } else {
                List<StripeColumnMeta> stripeColumnMetaList = stripeColumnMetaMap.values().stream().filter(x -> {
                    DoubleColumnStatistics statistics = (DoubleColumnStatistics) x.getColumnStatistics();
                    return !(value >= statistics.getMaximum());
                }).collect(Collectors.toList());
                return generatePruningResult(stripeColumnMetaList, stripeColumnMetaMap);
            }
        } else if (predicateLeaf.getOperator() == PredicateLeaf.Operator.GREATER_THAN_EQUALS) {
            Object literal = predicateLeaf.getLiteral();

            double value = ((Number) literal).doubleValue();

            if (value > statisticsMaximum) {
                return PruningResult.SKIP;
            } else {
                List<StripeColumnMeta> stripeColumnMetaList = stripeColumnMetaMap.values().stream().filter(x -> {
                    DoubleColumnStatistics statistics = (DoubleColumnStatistics) x.getColumnStatistics();
                    return !(value > statistics.getMaximum());
                }).collect(Collectors.toList());
                return generatePruningResult(stripeColumnMetaList, stripeColumnMetaMap);
            }
        } else {
            // TODO: support more predicate for pruning
            return PruningResult.PASS;
        }
    }

    public static PruningResult pruneLong(PredicateLeaf predicateLeaf, ColumnStatistics columnStatistics,
                                          Map<Long, StripeColumnMeta> stripeColumnMetaMap) {
        IntegerColumnStatistics integerColumnStatistics = ((IntegerColumnStatistics) columnStatistics);
        long statisticsMinimum = integerColumnStatistics.getMinimum();
        long statisticsMaximum = integerColumnStatistics.getMaximum();

        if (predicateLeaf.getOperator() == PredicateLeaf.Operator.EQUALS) {
            Object literal = predicateLeaf.getLiteral();

            long value = ((Number) literal).longValue();

            if (value > statisticsMaximum || value < statisticsMinimum) {
                return PruningResult.SKIP;
            }

            List<StripeColumnMeta> stripeColumnMetaList = stripeColumnMetaMap.values().stream().filter(x -> {
                IntegerColumnStatistics statistics = (IntegerColumnStatistics) x.getColumnStatistics();
                return !(value > statistics.getMaximum() || value < statistics.getMinimum());
            }).filter(x -> x.getBloomFilter() == null ? true : x.getBloomFilter().testLong(value)).collect(Collectors.toList());
            return generatePruningResult(stripeColumnMetaList, stripeColumnMetaMap);
        } else if (predicateLeaf.getOperator() == PredicateLeaf.Operator.IN) {
            List<Object> literalList = predicateLeaf.getLiteralList();

            List<StripeColumnMeta> stripeColumnMetaList = stripeColumnMetaMap.values().stream().filter(
                x -> {
                    IntegerColumnStatistics statistics = (IntegerColumnStatistics) x.getColumnStatistics();
                    for (Object literal : literalList) {
                        long value = ((Number) literal).longValue();
                        if (!(value > statistics.getMaximum() || value < statistics.getMinimum())) {
                            return true;
                        }
                    }
                    return false;
                }
            ).filter(
                x -> {
                    if (x.getBloomFilter() == null) {
                        return true;
                    }
                    for (Object literal : literalList) {
                        boolean test = x.getBloomFilter().testLong(((Number) literal).longValue());
                        if (test) {
                            return true;
                        }
                    }
                    return false;
                }
            ).collect(Collectors.toList());
            return generatePruningResult(stripeColumnMetaList, stripeColumnMetaMap);
        } else if (predicateLeaf.getOperator() == PredicateLeaf.Operator.IS_NULL) {
            boolean test = columnStatistics.hasNull();
            if (!test) {
                return PruningResult.SKIP;
            } else {
                List<StripeColumnMeta> stripeColumnMetaList =
                    stripeColumnMetaMap.values().stream()
                        .filter(x -> x.getColumnStatistics().hasNull()).collect(Collectors.toList());
                return generatePruningResult(stripeColumnMetaList, stripeColumnMetaMap);
            }
        } else if (predicateLeaf.getOperator() == PredicateLeaf.Operator.BETWEEN) {
            List<Object> literalList = predicateLeaf.getLiteralList();
            long min = ((Number) literalList.get(0)).longValue();
            long max = ((Number) literalList.get(1)).longValue();

            if (max < min) {
                return PruningResult.SKIP;
            }

            if (min > statisticsMaximum || max < statisticsMinimum) {
                return PruningResult.SKIP;
            } else {
                List<StripeColumnMeta> stripeColumnMetaList = stripeColumnMetaMap.values().stream().filter(x -> {
                    IntegerColumnStatistics statistics = (IntegerColumnStatistics) x.getColumnStatistics();
                    return !(min > statistics.getMaximum() || max < statistics.getMinimum());
                }).collect(Collectors.toList());
                return generatePruningResult(stripeColumnMetaList, stripeColumnMetaMap);
            }
        } else if (predicateLeaf.getOperator() == PredicateLeaf.Operator.LESS_THAN) {
            Object literal = predicateLeaf.getLiteral();

            long value = ((Number) literal).longValue();

            if (value <= statisticsMinimum) {
                return PruningResult.SKIP;
            } else {
                List<StripeColumnMeta> stripeColumnMetaList = stripeColumnMetaMap.values().stream().filter(x -> {
                    IntegerColumnStatistics statistics = (IntegerColumnStatistics) x.getColumnStatistics();
                    return !(value <= statistics.getMinimum());
                }).collect(Collectors.toList());
                return generatePruningResult(stripeColumnMetaList, stripeColumnMetaMap);
            }
        } else if (predicateLeaf.getOperator() == PredicateLeaf.Operator.LESS_THAN_EQUALS) {
            Object literal = predicateLeaf.getLiteral();

            long value = ((Number) literal).longValue();

            if (value < statisticsMinimum) {
                return PruningResult.SKIP;
            } else {
                List<StripeColumnMeta> stripeColumnMetaList = stripeColumnMetaMap.values().stream().filter(x -> {
                    IntegerColumnStatistics statistics = (IntegerColumnStatistics) x.getColumnStatistics();
                    return !(value < statistics.getMinimum());
                }).collect(Collectors.toList());
                return generatePruningResult(stripeColumnMetaList, stripeColumnMetaMap);
            }
        } else if (predicateLeaf.getOperator() == PredicateLeaf.Operator.GREATER_THAN) {
            Object literal = predicateLeaf.getLiteral();

            long value = ((Number) literal).longValue();

            if (value >= statisticsMaximum) {
                return PruningResult.SKIP;
            } else {
                List<StripeColumnMeta> stripeColumnMetaList = stripeColumnMetaMap.values().stream().filter(x -> {
                    IntegerColumnStatistics statistics = (IntegerColumnStatistics) x.getColumnStatistics();
                    return !(value >= statistics.getMaximum());
                }).collect(Collectors.toList());
                return generatePruningResult(stripeColumnMetaList, stripeColumnMetaMap);
            }
        } else if (predicateLeaf.getOperator() == PredicateLeaf.Operator.GREATER_THAN_EQUALS) {
            Object literal = predicateLeaf.getLiteral();

            long value = ((Number) literal).longValue();

            if (value > statisticsMaximum) {
                return PruningResult.SKIP;
            } else {
                List<StripeColumnMeta> stripeColumnMetaList = stripeColumnMetaMap.values().stream().filter(x -> {
                    IntegerColumnStatistics statistics = (IntegerColumnStatistics) x.getColumnStatistics();
                    return !(value > statistics.getMaximum());
                }).collect(Collectors.toList());
                return generatePruningResult(stripeColumnMetaList, stripeColumnMetaMap);
            }
        } else {
            // TODO: support more predicate for pruning
            return PruningResult.PASS;
        }
    }

    public static byte[] decimalToBin(DecimalStructure dec, int precision, int scale) {
        byte[] result = new byte[DecimalConverter.binarySize(precision, scale)];
        DecimalConverter.decimalToBin(dec, result, precision, scale);
        return MySQLUnicodeUtils.latin1ToUtf8(result).getBytes();
    }

    public static PruningResult pruneDecimal(PredicateLeaf predicateLeaf, ColumnStatistics columnStatistics,
                                             Map<Long, StripeColumnMeta> stripeColumnMetaMap) {
        StringColumnStatistics stringColumnStatistics = ((StringColumnStatistics) columnStatistics);
        String statisticsMinimum = stringColumnStatistics.getMinimum();
        String statisticsMaximum = stringColumnStatistics.getMaximum();

        if (statisticsMinimum == null || statisticsMaximum == null) {
            return PruningResult.SKIP;
        }

        if (predicateLeaf.getOperator() == PredicateLeaf.Operator.EQUALS) {
            String value = (String) predicateLeaf.getLiteral();
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);

            if (value.compareTo(statisticsMaximum) > 0 ||
                value.compareTo(statisticsMinimum) < 0) {
                return PruningResult.SKIP;
            }

            List<StripeColumnMeta> stripeColumnMetaList = stripeColumnMetaMap.values().stream().filter(x -> {
                StringColumnStatistics statistics = (StringColumnStatistics) x.getColumnStatistics();
                if (statistics.getMinimum() == null || statistics.getMaximum() == null) {
                    return false;
                }
                return !(value.compareTo(statistics.getMaximum()) > 0 ||
                    value.compareTo(statistics.getMinimum()) < 0);
            }).filter(x -> x.getBloomFilter() == null ? true : x.getBloomFilter().testBytes(bytes, 0, bytes.length)).collect(Collectors.toList());
            return generatePruningResult(stripeColumnMetaList, stripeColumnMetaMap);
        } else if (predicateLeaf.getOperator() == PredicateLeaf.Operator.IN) {
            // bloom filter
            List<Object> literalList = predicateLeaf.getLiteralList();

            List<StripeColumnMeta> stripeColumnMetaList = stripeColumnMetaMap.values().stream().filter(
                x -> {
                    StringColumnStatistics statistics = (StringColumnStatistics) x.getColumnStatistics();
                    if (statistics.getMinimum() == null || statistics.getMaximum() == null) {
                        return false;
                    }
                    for (Object literal : literalList) {
                        String value = (String) literal;
                        if (!(value.compareTo(statistics.getMaximum()) > 0 ||
                            value.compareTo(statistics.getMinimum()) < 0)) {
                            return true;
                        }
                    }
                    return false;
                }
            ).filter(
                x -> {
                    if (x.getBloomFilter() == null) {
                        return true;
                    }
                    for (Object literal : literalList) {
                        String value = (String) literal;
                        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
                        boolean test = x.getBloomFilter().testBytes(bytes, 0, bytes.length);
                        if (test) {
                            return true;
                        }
                    }
                    return false;
                }
            ).collect(Collectors.toList());
            return generatePruningResult(stripeColumnMetaList, stripeColumnMetaMap);
        } else if (predicateLeaf.getOperator() == PredicateLeaf.Operator.IS_NULL) {
            boolean test = columnStatistics.hasNull();
            if (!test) {
                return PruningResult.SKIP;
            } else {
                List<StripeColumnMeta> stripeColumnMetaList =
                    stripeColumnMetaMap.values().stream()
                        .filter(x -> x.getColumnStatistics().hasNull()).collect(Collectors.toList());
                return generatePruningResult(stripeColumnMetaList, stripeColumnMetaMap);
            }
        } else if (predicateLeaf.getOperator() == PredicateLeaf.Operator.BETWEEN) {
            List<Object> literalList = predicateLeaf.getLiteralList();
            String min = (String) literalList.get(0);
            String max = (String) literalList.get(1);

            if (max.compareTo(min) < 0) {
                return PruningResult.SKIP;
            }

            if (min.compareTo(statisticsMaximum) > 0 ||
                max.compareTo(statisticsMinimum) < 0) {
                return PruningResult.SKIP;
            } else {
                List<StripeColumnMeta> stripeColumnMetaList = stripeColumnMetaMap.values().stream().filter(x -> {
                    StringColumnStatistics statistics = (StringColumnStatistics) x.getColumnStatistics();
                    if (statistics.getMinimum() == null || statistics.getMaximum() == null) {
                        return false;
                    }
                    return !(min.compareTo(statistics.getMaximum()) > 0 ||
                        max.compareTo(statistics.getMinimum()) < 0);
                }).collect(Collectors.toList());

                return generatePruningResult(stripeColumnMetaList, stripeColumnMetaMap);
            }
        } else if (predicateLeaf.getOperator() == PredicateLeaf.Operator.LESS_THAN) {
            String value = (String) predicateLeaf.getLiteral();

            if (value.compareTo(statisticsMinimum) <= 0) {
                return PruningResult.SKIP;
            } else {
                List<StripeColumnMeta> stripeColumnMetaList = stripeColumnMetaMap.values().stream().filter(x -> {
                    StringColumnStatistics statistics = (StringColumnStatistics) x.getColumnStatistics();
                    if (statistics.getMinimum() == null || statistics.getMaximum() == null) {
                        return false;
                    }
                    return !(value.compareTo(statistics.getMinimum()) <= 0);
                }).collect(Collectors.toList());

                return generatePruningResult(stripeColumnMetaList, stripeColumnMetaMap);
            }
        } else if (predicateLeaf.getOperator() == PredicateLeaf.Operator.LESS_THAN_EQUALS) {
            String value = (String) predicateLeaf.getLiteral();

            if (value.compareTo(statisticsMinimum) < 0) {
                return PruningResult.SKIP;
            } else {
                List<StripeColumnMeta> stripeColumnMetaList = stripeColumnMetaMap.values().stream().filter(x -> {
                    StringColumnStatistics statistics = (StringColumnStatistics) x.getColumnStatistics();
                    if (statistics.getMinimum() == null || statistics.getMaximum() == null) {
                        return false;
                    }
                    return !(value.compareTo(statistics.getMinimum()) < 0);
                }).collect(Collectors.toList());

                return generatePruningResult(stripeColumnMetaList, stripeColumnMetaMap);
            }
        } else if (predicateLeaf.getOperator() == PredicateLeaf.Operator.GREATER_THAN) {
            String value = (String) predicateLeaf.getLiteral();

            if (value.compareTo(statisticsMaximum) >= 0) {
                return PruningResult.SKIP;
            } else {
                List<StripeColumnMeta> stripeColumnMetaList = stripeColumnMetaMap.values().stream().filter(x -> {
                    StringColumnStatistics statistics = (StringColumnStatistics) x.getColumnStatistics();
                    if (statistics.getMinimum() == null || statistics.getMaximum() == null) {
                        return false;
                    }
                    return !(value.compareTo(statistics.getMaximum()) >= 0);
                }).collect(Collectors.toList());

                return generatePruningResult(stripeColumnMetaList, stripeColumnMetaMap);
            }
        } else if (predicateLeaf.getOperator() == PredicateLeaf.Operator.GREATER_THAN_EQUALS) {
            String value = (String) predicateLeaf.getLiteral();

            if (value.compareTo(statisticsMaximum) > 0) {
                return PruningResult.SKIP;
            } else {
                List<StripeColumnMeta> stripeColumnMetaList = stripeColumnMetaMap.values().stream().filter(x -> {
                    StringColumnStatistics statistics = (StringColumnStatistics) x.getColumnStatistics();
                    if (statistics.getMinimum() == null || statistics.getMaximum() == null) {
                        return false;
                    }
                    return !(value.compareTo(statistics.getMaximum()) > 0);
                }).collect(Collectors.toList());

                return generatePruningResult(stripeColumnMetaList, stripeColumnMetaMap);
            }
        } else {
            // TODO: support more predicate for pruning
            return PruningResult.PASS;
        }
    }

    private static PruningResult generatePruningResult(List<StripeColumnMeta> stripeColumnMetaList,
                                                       Map<Long, StripeColumnMeta> stripeColumnMetaMap) {
        if (stripeColumnMetaMap.isEmpty()) {
            // no statistic
            return PruningResult.PASS;
        } else if (stripeColumnMetaList.size() == stripeColumnMetaMap.size()) {
            return PruningResult.PASS;
        } else if (stripeColumnMetaList.isEmpty()) {
            return PruningResult.SKIP;
        } else {
            return new PruningResult(stripeColumnMetaList);
        }
    }
}
