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

package com.alibaba.polardbx.executor.balancer.splitpartition;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.balancer.stats.PartitionStat;
import com.alibaba.polardbx.executor.balancer.stats.StatsUtils;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.field.SessionProperties;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundVal;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundValueKind;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionFieldBuilder;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionIntFunction;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumComparator;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;
import com.alibaba.polardbx.optimizer.partition.util.PartTupleRouter;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.*;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.properties.ConnectionProperties.ENABLE_AUTO_SPLIT_PARTITION;
import static java.lang.Math.max;

/**
 * Utilities for SplitPoint
 *
 * @author moyi
 * @since 2021/04
 */
public class SplitPointUtils {

    final private static String SQL_CHECK_FEATURE_SUPPORTED =
        "show variables like 'innodb_innodb_btree_sampling'";

    /**
     * Sort split-points and duplicate
     */
    public static List<SplitPoint> sortAndDuplicate(PartitionStat partition, List<SplitPoint> originSp) {
        List<SplitPoint> result = new ArrayList<>();
        SearchDatumComparator comparator = partition.getPartitionBy().getPruningSpaceComparator();
        originSp.sort(new SplitPointComparator(comparator));

        SearchDatumInfo last = null;
        if (partition.getPosition() > 1) {
            last = partition.getPrevBound();
        }
        for (SplitPoint sp : originSp) {
            if (last == null) {
                result.add(sp);
            } else if (comparator.compare(last, sp.getValue()) < 0) {
                result.add(sp);
            }
            last = sp.getValue();
        }

        // re-assign partition name
        SplitNameBuilder snb = new SplitNameBuilder(partition.getPartitionName());
        for (SplitPoint splitPoint : result) {
            snb.build(splitPoint);
        }
        return result;
    }

    /**
     * Choose a part of split-points using sampling, try best to choose uniformed value.
     * Input split-points should already be sorted
     */
    public static List<SplitPoint> sample(String partitionName, List<SplitPoint> splitPoints, int maxCount) {
        if (splitPoints.size() <= maxCount) {
            return splitPoints;
        }
        List<SplitPoint> result = new ArrayList<>(maxCount);
        SplitNameBuilder snb = new SplitNameBuilder(partitionName);
        double step = splitPoints.size() * 1.0 / maxCount;
        double gap = 0.0;
        for (SplitPoint splitPoint : splitPoints) {
            if (gap >= step) {
                SplitPoint newSp = splitPoint.clone();
                snb.build(newSp);
                result.add(newSp);
                gap = 0;
            }
            gap += 1;
        }
        return result;
    }

    /**
     * Query a physical partition of a table, the sql should use physical table name
     */
    public static List<SearchDatumInfo> queryTablePartition(PartitionStat partition, String sql) {
        String schema = partition.getSchema();
        String physicalDatabase = partition.getPhysicalDatabase();
        List<DataType> columnTypes = partition.getPartitionBy().getPartitionColumnTypeList();

        return StatsUtils.queryGroupTyped(schema, physicalDatabase, columnTypes, sql);
    }

    public static boolean supportSampling(PartitionStat partition) {
        String schema = partition.getSchema();
        String physicalDb = partition.getPhysicalDatabase();
        List<List<Object>> res = StatsUtils.queryGroupByPhyDb(schema, physicalDb, SQL_CHECK_FEATURE_SUPPORTED);
        return res.stream().anyMatch(row -> row.size() >= 2 && "ON".equals(row.get(1)));

    }

    public static boolean supportStatistics(PartitionStat partition) {
        return DdlHelper.getInstConfigAsBoolean(SQLRecorderLogger.ddlEngineLogger, ENABLE_AUTO_SPLIT_PARTITION, true);
    }

    /**
     * Evaluate expression or hash for such partition strategies:
     * RANGE(year(id)), HASH(year(id)),
     * HASH(id), KEY(id1, id2)
     */
    public static SearchDatumInfo generateSplitBound(PartitionByDefinition partitionBy,
                                                     SearchDatumInfo actualPartitionKey) {
        SearchDatumInfo result = actualPartitionKey;

        PartitionIntFunction func = partitionBy.getPartIntFunc();
        if (func != null) {
            PartitionField actualValue = actualPartitionKey.getSingletonValue().getValue();
            long value = func.evalInt(actualValue, SessionProperties.empty());
            result = SearchDatumInfo.createFromHashCode(value);
        }

        if (partitionBy.getStrategy() == PartitionStrategy.HASH) {
            long hashCode = partitionBy.getHasher().calcHashCodeForHashStrategy(result);
            result = SearchDatumInfo.createFromHashCode(hashCode);
        } else if (partitionBy.getStrategy() == PartitionStrategy.KEY) {
            Long[] hashCodes = partitionBy.getHasher().calcHashCodeForKeyStrategy(result);
            result = SearchDatumInfo.createFromHashCodes(hashCodes);
        }

        return result;
    }

    public static List<SearchDatumInfo> generateSplitBounds(final String tableSchema,
                                                            final String logicalTableName,
                                                            final String partName,
                                                            final int splitCount,
                                                            final long maxPartitionSize) {
        ExecutionContext ec = new ExecutionContext();
        ec.setParams(new Parameters());
        ec.setSchemaName(tableSchema);
        ec.setServerVariables(new HashMap<>());
        PartitionInfo partitionInfo =
            OptimizerContext.getContext(tableSchema).getPartitionInfoManager().getPartitionInfo(logicalTableName);
        List<Pair<List<Object>, SearchDatumInfo>> rowValues2SearchDatums = new ArrayList<>();
        SplitPartitionStats splitPartitionStats =
            SplitPartitionStats.createForSplitPartition(tableSchema, logicalTableName, partName);
        splitPartitionStats.prepare();
        List<List<Object>> sampleRows;
        try {
            sampleRows = splitPartitionStats.sampleTablePartitions().getKey();
        } catch (SQLException e) {
            return null;
        }
        if (sampleRows == null) {
            return null;
        }
        PartTupleRouter tupleRouter = new PartTupleRouter(partitionInfo, ec);
        tupleRouter.init();
        for (int i = 0; i < sampleRows.size(); i++) {
            //todo support subpartition
            List<SearchDatumInfo> searchDatumInfos = tupleRouter.calcSearchDatum(Arrays.asList(sampleRows.get(i)));
            rowValues2SearchDatums.add(new Pair(sampleRows.get(i), searchDatumInfos.get(0)));
        }
        Collections.sort(rowValues2SearchDatums,
            (r1, r2) -> partitionInfo.getPartitionBy().getBoundSpaceComparator().compare(r1.getValue(), r2.getValue()));

        int splitSize = max(1, rowValues2SearchDatums.size() / splitCount);
        List<SearchDatumInfo> splitPoints = new ArrayList<>();
        int lastRow = 0;
        int row = lastRow + splitSize;
        while (row < sampleRows.size() && lastRow < sampleRows.size()) {
            SearchDatumInfo comparedSearchDatumInfo = rowValues2SearchDatums.get(row).getValue();
            SearchDatumInfo searchDatumInfo = null;
            int j = row;
            for (; j > lastRow; j--) {
                searchDatumInfo = rowValues2SearchDatums.get(j).getValue();
                if (partitionInfo.getPartitionBy().getBoundSpaceComparator()
                    .compare(comparedSearchDatumInfo, searchDatumInfo) != 0) {
                    j++;
                    break;
                }
            }
            if (j == lastRow) {
                for (j = row; j < sampleRows.size(); j++) {
                    searchDatumInfo = rowValues2SearchDatums.get(j).getValue();
                    if (partitionInfo.getPartitionBy().getBoundSpaceComparator()
                        .compare(comparedSearchDatumInfo, searchDatumInfo) != 0) {
                        break;
                    }
                }
            }
            lastRow = j;
            if (lastRow >= sampleRows.size()) {
                break;
            }
            splitPoints.add(rowValues2SearchDatums.get(lastRow).getValue());
            if (lastRow + splitSize >= sampleRows.size() - 1) {
                break;
            }
            row = lastRow + splitSize;
        }
        return splitPoints;
    }

    public static Pair<List<SearchDatumInfo>, List<Double>> generateSplitBounds(final String tableSchema,
                                                                                final String logicalTableName,
                                                                                final String partName,
                                                                                final List<Double> splitSizes,
                                                                                final Boolean nonHotSplit) {
        Boolean usePartialSearchDatum = false;
        ExecutionContext ec = new ExecutionContext();
        ec.setParams(new Parameters());
        ec.setSchemaName(tableSchema);
        ec.setServerVariables(new HashMap<>());

        Double partitionSize = splitSizes.stream().reduce(Double::sum).get();

        PartitionInfo partitionInfo =
            OptimizerContext.getContext(tableSchema).getPartitionInfoManager().getPartitionInfo(logicalTableName);
        List<Pair<List<Object>, SearchDatumInfo>> rowValues2SearchDatums = new ArrayList<>();
        SplitPartitionStats splitPartitionStats =
            SplitPartitionStats.createForSplitPartition(tableSchema, logicalTableName, partName);
        splitPartitionStats.prepare();
        List<List<Object>> sampleRows;
        try {
            sampleRows = splitPartitionStats.sampleTablePartitions().getKey();
        } catch (SQLException e) {
            return null;
        }
        if (sampleRows == null) {
            return null;
        }
        if (partitionInfo.getPartitionBy().getSubPartitionBy() != null) {
            throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT,
                "Not support to generate split bounds of subpartition table");
        }
        PartTupleRouter tupleRouter = new PartTupleRouter(partitionInfo, ec);
        tupleRouter.init();
        if (partitionInfo.getPartitionBy().getStrategy().isKey() && partitionInfo.getPartitionColumns().size() > 1
            && nonHotSplit) {
            usePartialSearchDatum = true;
            int actualPartKeyNum = partitionInfo.getPartitionBy().getAllLevelActualPartCols().get(0).size();
            for (int i = 0; i < sampleRows.size(); i++) {
                List<List<Object>> allLevelSampleRows = new ArrayList<>();
                allLevelSampleRows.add(sampleRows.get(i));

                //SearchDatumInfo searchDatumInfo = tupleRouter.calcSearchDatum(sampleRows.get(i));
                List<SearchDatumInfo> allLevelSearchDatumInfo = tupleRouter.calcSearchDatum(allLevelSampleRows);
                SearchDatumInfo searchDatumInfo = allLevelSearchDatumInfo.get(0);

                PartitionBoundVal[] partitionBoundVals =
                    Arrays.copyOfRange(searchDatumInfo.getDatumInfo(), 0, actualPartKeyNum);
                rowValues2SearchDatums.add(new Pair(sampleRows.get(i), new SearchDatumInfo(partitionBoundVals)));
            }
            Collections.sort(rowValues2SearchDatums,
                (r1, r2) -> partitionInfo.getPartitionBy().getBoundSpaceComparator()
                    .compare(r1.getValue(), r2.getValue()));
        } else {
            for (int i = 0; i < sampleRows.size(); i++) {
                List<List<Object>> allLevelSampleRows = new ArrayList<>();
                allLevelSampleRows.add(sampleRows.get(i));
                //SearchDatumInfo searchDatumInfo = tupleRouter.calcSearchDatum(allLevelSampleRows);
                List<SearchDatumInfo> allLevelSearchDatumInfo = tupleRouter.calcSearchDatum(allLevelSampleRows);
                rowValues2SearchDatums.add(new Pair(sampleRows.get(i), allLevelSearchDatumInfo.get(0)));
            }
            Collections.sort(rowValues2SearchDatums,
                (r1, r2) -> partitionInfo.getPartitionBy().getBoundSpaceComparator()
                    .compare(r1.getValue(), r2.getValue()));
        }

        List<Integer> estimatedSplitSize =
            splitSizes.stream().map(o -> o / partitionSize * rowValues2SearchDatums.size())
                .map(o -> Math.max(o.intValue(), 1)).collect(Collectors.toList());
        List<SearchDatumInfo> splitPoints = new ArrayList<>();
        List<Double> finalSplitSize = new ArrayList<>();
        int partCount = 0;
        int lastRow = 0;
        int row = lastRow + estimatedSplitSize.get(partCount);
        while (row < sampleRows.size() && lastRow < sampleRows.size()) {
            SearchDatumInfo comparedSearchDatumInfo = rowValues2SearchDatums.get(row).getValue();
            SearchDatumInfo searchDatumInfo = null;
            int j = row;
            for (; j > lastRow; j--) {
                searchDatumInfo = rowValues2SearchDatums.get(j).getValue();
                if (partitionInfo.getPartitionBy().getBoundSpaceComparator()
                    .compare(comparedSearchDatumInfo, searchDatumInfo) != 0) {
                    j++;
                    break;
                }
            }
            if (j == lastRow) {
                for (j = row; j < sampleRows.size(); j++) {
                    searchDatumInfo = rowValues2SearchDatums.get(j).getValue();
                    if (partitionInfo.getPartitionBy().getBoundSpaceComparator()
                        .compare(comparedSearchDatumInfo, searchDatumInfo) != 0) {
                        break;
                    }
                }
            }
            lastRow = j;
            if (lastRow >= sampleRows.size()) {
                break;
            }
            if (usePartialSearchDatum) {
                SearchDatumInfo searchDatumInfo1 =
                    chooseSplitPointForSearchDatum(rowValues2SearchDatums.get(lastRow).getValue());
                splitPoints.add(searchDatumInfo1);
            } else {
                splitPoints.add(rowValues2SearchDatums.get(lastRow).getValue());
            }
            finalSplitSize.add((double) lastRow);
            partCount++;
            if (partCount >= splitSizes.size()
                || lastRow + estimatedSplitSize.get(partCount) >= sampleRows.size() - 1) {
                break;
            }
            row = lastRow + estimatedSplitSize.get(partCount);
        }
        finalSplitSize = finalSplitSize.stream().map(o -> o * partitionSize / rowValues2SearchDatums.size()).collect(
            Collectors.toList());
        return Pair.of(splitPoints, finalSplitSize);
    }

    public static SearchDatumInfo chooseSplitPointForSearchDatum(SearchDatumInfo searchDatumInfo) {
        // Assert: sizeOf({search_datum | search_datum  >= split_point}) > 0
        // if partition key is (X, Y) for searchDatum (x, y) , search_datum >= split_point, we would expected (x, y) as split point.
        // if partition key is (X, Y, Z) for search_datum (x, y), split_point (x, y) = (x, y, MAX_LONG_VALUE), so search_datum < split_point.
        // then we would set split_point (x, y - 1) = (x, y - 1, MAX_LONG_VALUE) as split point.
        // if partition key is (X, Y, Z) for searchDatum (x, MIN_VALUE), split_point (x - 1, MAX_VALUE)
        PartitionBoundVal[] partitionBoundVals = searchDatumInfo.getDatumInfo();
        int i;
        PartitionBoundVal lastBoundVal = null;
        for (i = partitionBoundVals.length - 1; i >= 0; i--) {
            lastBoundVal = partitionBoundVals[i];
            if (lastBoundVal.getValue().longValue() != Long.MIN_VALUE) {
                break;
            }
        }
        if (i < 0) {
            return searchDatumInfo;
        } else {
            Long lastBoundLongValue = lastBoundVal.getValue().longValue();
            PartitionBoundVal[] beforePartitionBoundVals =
                Arrays.copyOfRange(partitionBoundVals, 0, partitionBoundVals.length);
            PartitionField partFld = PartitionFieldBuilder.createField(DataTypes.LongType);
            partFld.store(lastBoundLongValue - 1, DataTypes.LongType);
            PartitionBoundVal beforeLastBoundVal =
                PartitionBoundVal.createPartitionBoundVal(partFld, PartitionBoundValueKind.DATUM_NORMAL_VALUE);
            beforePartitionBoundVals[i] = beforeLastBoundVal;
            for (int j = i + 1; j < partitionBoundVals.length; j++) {
                /**
                 * Hash/Key's maxvalue bound value should use Long.MAX_VALUE as bound
                 */
                beforePartitionBoundVals[j] = PartitionBoundVal.createHashBoundMaxValue();
            }
            return new SearchDatumInfo(beforePartitionBoundVals);

        }
    }
}


