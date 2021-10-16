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

package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.executor.balancer.stats.StatsUtils;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.filter.Like;
import com.alibaba.polardbx.optimizer.view.InformationSchemaTableDetail;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;

import java.math.BigInteger;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class InformationSchemaTableDetailHandler extends BaseVirtualViewSubClassHandler {
    public InformationSchemaTableDetailHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    private static char finishChar = '█';
    private static char unFinishChar = '-';

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaTableDetail;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        InformationSchemaTableDetail informationSchemaTableDetail = (InformationSchemaTableDetail) virtualView;
        List<TableGroupConfig> allTableGroupConfigs = StatsUtils.getTableGroupConfigs();
        // only new partitioning db
        Set<String> schemaNames = StatsUtils.getSchemaNames(allTableGroupConfigs);

        List<Object> tableSchemaIndexValue =
            virtualView.getIndex().get(informationSchemaTableDetail.getTableSchemaIndex());

        Object tableSchemaLikeValue =
            virtualView.getLike().get(informationSchemaTableDetail.getTableSchemaIndex());

        List<Object> tableNameIndexValue =
            virtualView.getIndex().get(informationSchemaTableDetail.getTableNameIndex());

        Object tableNameLikeValue =
            virtualView.getLike().get(informationSchemaTableDetail.getTableNameIndex());

        Map<Integer, ParameterContext> params = executionContext.getParams().getCurrentParameter();

        // schemaIndex
        Set<String> indexSchemaNames = new HashSet<>();
        if (tableSchemaIndexValue != null && !tableSchemaIndexValue.isEmpty()) {
            for (Object obj : tableSchemaIndexValue) {
                if (obj instanceof RexDynamicParam) {
                    String schemaName = String.valueOf(params.get(((RexDynamicParam) obj).getIndex() + 1).getValue());
                    indexSchemaNames.add(schemaName.toLowerCase());
                } else if (obj instanceof RexLiteral) {
                    String schemaName = ((RexLiteral) obj).getValueAs(String.class);
                    indexSchemaNames.add(schemaName.toLowerCase());
                }
            }
            schemaNames = schemaNames.stream()
                .filter(schemaName -> indexSchemaNames.contains(schemaName.toLowerCase()))
                .collect(Collectors.toSet());
        }

        // schemaLike
        String schemaLike = null;
        if (tableSchemaLikeValue != null) {
            if (tableSchemaLikeValue instanceof RexDynamicParam) {
                schemaLike =
                    String.valueOf(params.get(((RexDynamicParam) tableSchemaLikeValue).getIndex() + 1).getValue());
            } else if (tableSchemaLikeValue instanceof RexLiteral) {
                schemaLike = ((RexLiteral) tableSchemaLikeValue).getValueAs(String.class);
            }
            if (schemaLike != null) {
                final String likeArg = schemaLike;
                schemaNames = schemaNames.stream().filter(schemaName -> new Like(null, null).like(
                    schemaName, likeArg)).collect(
                    Collectors.toSet());
            }
        }

        // tableIndex
        Set<String> indexTableNames = new HashSet<>();
        if (tableNameIndexValue != null && !tableNameIndexValue.isEmpty()) {
            for (Object obj : tableNameIndexValue) {
                if (obj instanceof RexDynamicParam) {
                    String tableName = String.valueOf(params.get(((RexDynamicParam) obj).getIndex() + 1).getValue());
                    indexTableNames.add(tableName.toLowerCase());
                } else if (obj instanceof RexLiteral) {
                    String tableName = ((RexLiteral) obj).getValueAs(String.class);
                    indexTableNames.add(tableName.toLowerCase());
                }
            }
        }

        // tableLike
        String tableLike = null;
        if (tableNameLikeValue != null) {
            if (tableNameLikeValue instanceof RexDynamicParam) {
                tableLike =
                    String.valueOf(params.get(((RexDynamicParam) tableNameLikeValue).getIndex() + 1).getValue());
            } else if (tableNameLikeValue instanceof RexLiteral) {
                tableLike = ((RexLiteral) tableNameLikeValue).getValueAs(String.class);
            }
        }

        List<TableGroupConfig> tableGroupConfigs =
            StatsUtils.getTableGroupConfigsWithFilter(allTableGroupConfigs, schemaNames);

        // get all phy tables(partitions) info from all DNs
        Map<String, Map<String, List<Object>>> phyDbTablesInfo =
            StatsUtils.queryTableSchemaStats(schemaNames, indexTableNames, tableLike);

        for (TableGroupConfig tableGroupConfig : tableGroupConfigs) {
            if (tableGroupConfig.getTableCount() == 0) {
                continue;
            }
            String schemaName = tableGroupConfig.getTableGroupRecord().schema;

            Map<String, Map<String, List<Object>>> tablesStatInfo =
                StatsUtils.queryTableGroupStats(tableGroupConfig, indexTableNames, tableLike, phyDbTablesInfo);

            for (TablePartRecordInfoContext context : tableGroupConfig.getAllTables()) {
                String logicalTableName = context.getTableName().toLowerCase();
                if (!StatsUtils.isFilterTable(indexTableNames, tableLike, logicalTableName)) {
                    continue;
                }

                List<TablePartitionRecord> tablePartitionRecords =
                    context.getPartitionRecList().stream().filter(
                        o -> (o.partLevel != TablePartitionRecord.PARTITION_LEVEL_LOGICAL_TABLE)).collect(
                        Collectors.toList());
                Map<String, List<Object>> tableStatInfo =
                    tablesStatInfo.get(context.getLogTbRec().tableName.toLowerCase());
                Long totalRows = 0L;
                for (Map.Entry<String, List<Object>> phyEntry : tableStatInfo.entrySet()) {
                    // FIXME the value could be either BigInteger or Long
                    if (phyEntry.getValue().get(3) instanceof BigInteger) {
                        totalRows += ((BigInteger) phyEntry.getValue().get(3)).longValue();
                    } else if (phyEntry.getValue().get(3) instanceof Long) {
                        totalRows += (Long) phyEntry.getValue().get(3);
                    }
                }
                for (int i = 0; i < tablePartitionRecords.size(); i++) {
                    TablePartitionRecord record = tablePartitionRecords.get(i);
                    // The value could be either BigInteger, BigDecimal or Long
                    Object obj = tableStatInfo.get(record.phyTable.toLowerCase()).get(3);
                    Long tableRow = DataTypes.LongType.convertFrom(obj);
                    double percent = tableRow.doubleValue() / Math.max(totalRows.doubleValue(), 1);

                    List<Object> statInfo = tableStatInfo.get(record.getPhyTable().toLowerCase());

                    // TODO(moyi) refactor it
                    Object[] row = new Object[15];
                    cursor.addRow(row);
                    row[0] = DataTypes.StringType.convertFrom(schemaName);
                    row[1] = DataTypes.StringType.convertFrom(tableGroupConfig.getTableGroupRecord().tg_name);
                    row[2] = DataTypes.StringType.convertFrom(record.tableName);
                    row[3] = DataTypes.StringType.convertFrom(record.phyTable);

                    row[4] = DataTypes.ULongType.convertFrom(i);
                    row[5] = DataTypes.StringType.convertFrom(record.partName);
                    row[6] = DataTypes.ULongType.convertFrom(tableRow);
                    row[7] =
                        DataTypes.ULongType.convertFrom(tableStatInfo.get(record.phyTable.toLowerCase()).get(4));
                    row[8] =
                        DataTypes.ULongType.convertFrom(tableStatInfo.get(record.phyTable.toLowerCase()).get(5));
                    row[9] =
                        DataTypes.StringType.convertFrom(tableStatInfo.get(record.phyTable.toLowerCase()).get(0));
                    row[10] = DataTypes.StringType.convertFrom(getPercentString(percent));

                    if (statInfo.size() > 6) {
                        for (int k = 6; k < 10; k++) {
                            if (statInfo.get(k) != null) {
                                row[k + 5] = DataTypes.ULongType.convertFrom(statInfo.get(k));
                            }
                        }
                    }
                }
            }
        }
        return cursor;
    }

    private String getPercentString(double percent) {
        StringBuilder finish = new StringBuilder();
        StringBuilder unFinish = new StringBuilder();

        for (int i = 0; i < percent * 100 / 4; i++) {
            finish.append(finishChar);
        }
        for (int i = 0; i < (100 - percent * 100) / 4; i++) {
            unFinish.append(unFinishChar);
        }
        return String.format("%.2f%%├%s%s┤", percent * 100, finish.toString(), unFinish.toString());
    }
}
