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

package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.exception.TddlException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.balancer.splitpartition.SplitPartitionStats;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalDal;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;
import com.alibaba.polardbx.optimizer.partition.util.PartTupleRouter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlShowHotkey;
import org.apache.commons.lang.StringUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author youtianyu
 */
public class LogicalShowHotkeyHandler extends HandlerCommon {

    private static final Logger logger = LoggerFactory.getLogger(LogicalShowDbStatusHandler.class);

    static int MAX_HOTKEY_NUM = 5;
    static double PERCENTAGE = 0.5;
    static double MIN_PERCENTAGE = 0.05;
    static int MIN_ROWS = 10_000;

    public LogicalShowHotkeyHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalDal show = (LogicalDal) logicalPlan;
        final SqlShowHotkey showHotkey = (SqlShowHotkey) show.getNativeSqlNode();
        final SqlNode sqlTableName = showHotkey.getTable();
        final SqlNode sqlPartitionName = showHotkey.getPartition();

        String tableName = sqlTableName.toString();
        String partitionName = sqlPartitionName.toString();
        String schemaName = executionContext.getSchemaName();
        if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
            try {
                return handleNewPartitionTable(executionContext, schemaName, tableName, partitionName);
            } catch (TddlException e) {
                throw new RuntimeException(e);
            }
        } else {
            return null;
        }
    }

    private ArrayResultCursor getHotkeyResultCursor() {
        ArrayResultCursor result = new ArrayResultCursor("HOTKEY");
        result.addColumn("SCHEMA_NAME", DataTypes.StringType);
        result.addColumn("TABLE_NAME", DataTypes.StringType);
        result.addColumn("PART_NAME", DataTypes.StringType);
        result.addColumn("HOTVALUE", DataTypes.StringType);
        result.addColumn("ESTIMATE_ROWS", DataTypes.LongType);
        result.addColumn("ESTIMATE_PERCENTAGE", DataTypes.StringType);
        result.initMeta();
        return result;
    }

    private Cursor handleNewPartitionTable(ExecutionContext executionContext, String schemaName, String tableName,
                                           String partitionName)
        throws TddlException {
        ArrayResultCursor result = getHotkeyResultCursor();
        final OptimizerContext context = OptimizerContext.getContext(schemaName);

        TableMeta tableMeta = context.getLatestSchemaManager().getTable(tableName);

        final PartitionInfo partitionInfo = tableMeta.getPartitionInfo();

        if (partitionInfo == null) {
            logger.error("Partition table " + tableName + " does not have partition info");
            return result;
        } else if (partitionInfo.getPartitionBy().getPartitionByPartName(partitionName) == null) {
            String errorInfo = "Partition table " + tableName + " partition " + partitionName + " does not exists";
            logger.error(errorInfo);
            throw new TddlException(ErrorCode.ERR_GMS_CHECK_ARGUMENTS, errorInfo);
        } else if (partitionInfo.getPartitionBy().getSubPartitionBy() != null) {
            throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT,
                "Not support to show hot key for subpartition table");
        }

        List<Pair<List<Object>, SearchDatumInfo>> rowValues2SearchDatums = new ArrayList<>();
        SplitPartitionStats splitPartitionStats =
            SplitPartitionStats.createForSplitPartition(schemaName, tableName, partitionName);
        splitPartitionStats.prepare();
        List<List<Object>> sampleRows;
        Long estimatedTotalRows;
        try {
            Pair<List<List<Object>>, Long> sampleRowsResult = splitPartitionStats.sampleTablePartitions();
            sampleRows = sampleRowsResult.getKey();
            estimatedTotalRows = sampleRowsResult.getValue();
        } catch (SQLException e) {
            logger.error("Partition table " + tableName + " sample failed");
            return result;
        }
        if (sampleRows == null || sampleRows.isEmpty()) {
            logger.error("Partition table " + tableName + " sample failed");
            return result;
        }
        PartTupleRouter tupleRouter = new PartTupleRouter(partitionInfo, executionContext);
        tupleRouter.init();
        for (int i = 0; i < sampleRows.size(); i++) {
            List<List<Object>> allLevelSampleRows = new ArrayList<>();
            allLevelSampleRows.add(sampleRows.get(i));
            //SearchDatumInfo searchDatumInfo = tupleRouter.calcSearchDatum(sampleRows.get(i));
            List<SearchDatumInfo> allLevelSearchDatumInfos = tupleRouter.calcSearchDatum(allLevelSampleRows);
            rowValues2SearchDatums.add(new Pair(sampleRows.get(i), allLevelSearchDatumInfos.get(0)));
        }
        Collections.sort(rowValues2SearchDatums,
            (r1, r2) -> partitionInfo.getPartitionBy().getBoundSpaceComparator().compare(r1.getValue(), r2.getValue()));

        constructResult(result, partitionInfo, rowValues2SearchDatums, estimatedTotalRows, schemaName, tableName,
            partitionName);
        return result;
    }

    private void constructResult(ArrayResultCursor result, PartitionInfo partitionInfo,
                                 List<Pair<List<Object>, SearchDatumInfo>> rowValues2SearchDatums,
                                 Long estimateTotalRows, String schemaName, String tableName, String partName) {

        int rows = rowValues2SearchDatums.size();
        Map<List<Object>, Integer> keysCount = new HashMap<>();

        //int usedPartitionColumnsNum = PartitionInfoUtil.getActualPartitionColumns(partitionInfo).size();
        int usedPartitionColumnsNum = PartitionInfoUtil.getAllLevelActualPartColumns(partitionInfo).get(0).size();
        for (int i = 0; i < rowValues2SearchDatums.size(); i++) {
            List<Object> row = rowValues2SearchDatums.get(i).getKey();
            List<Object> usedKey = row.subList(0, usedPartitionColumnsNum);
            if (keysCount.containsKey(usedKey)) {
                keysCount.put(usedKey, keysCount.get(usedKey) + 1);
            } else {
                keysCount.put(usedKey, 1);
            }
        }
        List<List<Object>> sortedKeys =
            keysCount.keySet().stream().sorted(Comparator.comparingInt(keysCount::get).reversed()).collect(
                Collectors.toList());
        int sumCount = 0;
        for (int i = 0; i <= MAX_HOTKEY_NUM && i < sortedKeys.size(); i++) {
            int count = keysCount.get(sortedKeys.get(i));
            sumCount += count;
            List<String> keyValue = sortedKeys.get(i).stream().map(o -> o.toString()).collect(Collectors.toList());
            String keyString = String.format("(%s)", StringUtils.join(keyValue, ","));
            double estimatePercentage = count / (double) rows;
            String estimatePercentageString = String.format("%.2f%%", estimatePercentage * 100);
            long estimateRows = (long) Math.ceil(estimatePercentage * estimateTotalRows);
            if (estimatePercentage < MIN_PERCENTAGE || estimateRows < MIN_ROWS) {
                break;
            }
            result.addRow(new Object[] {
                schemaName,
                tableName,
                partName,
                keyString,
                estimateRows,
                estimatePercentageString
            });
            if (sumCount > rows * PERCENTAGE && estimatePercentage <= MIN_PERCENTAGE * (MAX_HOTKEY_NUM - 1)) {
                break;
            }
        }
    }
}
