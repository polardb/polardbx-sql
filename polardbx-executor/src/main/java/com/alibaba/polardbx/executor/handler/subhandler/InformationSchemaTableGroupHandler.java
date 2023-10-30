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

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.common.PartitionTableType;
import com.alibaba.polardbx.optimizer.sql.sql2rel.TddlSqlToRelConverter;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import com.alibaba.polardbx.optimizer.view.InformationSchemaFullTableGroup;
import com.alibaba.polardbx.optimizer.view.InformationSchemaTableGroup;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.util.List;
import java.util.Map;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class InformationSchemaTableGroupHandler extends BaseVirtualViewSubClassHandler {
    private static final Logger logger = LoggerFactory.getLogger(InformationSchemaTableGroupHandler.class);
    private static final String LOGICAL_GSI_NAME = "%s.%s";

    public InformationSchemaTableGroupHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaFullTableGroup;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        String schemaName = executionContext.getSchemaName();
        boolean isNewPart = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        if (!isNewPart) {
            throw new UnsupportedOperationException("unsupport command[show tablegroup]");
        }
        boolean isNotFull = virtualView instanceof InformationSchemaTableGroup;
        TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager();
        SchemaManager schemaManager = executionContext.getSchemaManager();
        final Map<Long, TableGroupConfig> tableGroupConfigMap =
            tableGroupInfoManager.getTableGroupConfigInfoCache();
        if (tableGroupConfigMap != null) {
            for (Map.Entry<Long, TableGroupConfig> entry : tableGroupConfigMap.entrySet()) {
                StringBuilder sb = new StringBuilder();
                TableGroupConfig tableGroupConfig = entry.getValue();
                TableGroupRecord tableGroupRecord = tableGroupConfig.getTableGroupRecord();
                String tgName = tableGroupConfig.getTableGroupRecord().getTg_name();
                String partitionString = "";
                int partCnt = 0;
                int subPartCnt = 0;

                if (isNotFull) {
                    int logTblCnt = 0;
                    int idxTblCnt = 0;
                    int tableCount = tableGroupConfig.getTableCount();

                    String actualPartStr = "";
                    String maxPartStr = "";

                    String actualSubPartStr = "";
                    String maxSubPartStr = "";
                    String subPartTempStr = "";

                    boolean isSingleTbOrBroadcastTb = false;
                    if (tableCount > 0) {
                        List<TablePartRecordInfoContext> tbInfoList = tableGroupConfig.getTables();
                        for (int i = 0; i < tbInfoList.size(); i++) {
                            String logTblName = tableGroupConfig.getTables().get(i).getLogTbRec().tableName;
                            TableMeta tableMeta = schemaManager.getTable(logTblName);
                            PartitionInfo partInfo = tableMeta.getPartitionInfo();
                            PartitionTableType tableType = partInfo.getTableType();
                            if (tableType == PartitionTableType.GSI_TABLE
                                || tableType == PartitionTableType.GSI_BROADCAST_TABLE
                                || tableType == PartitionTableType.GSI_SINGLE_TABLE) {
                                idxTblCnt++;
                            } else {
                                logTblCnt++;
                            }
                        }
                        String firstTblName = tableGroupConfig.getTables().get(0).getLogTbRec().tableName;
                        TableMeta tableMeta = schemaManager.getTable(firstTblName);
                        PartitionInfo firstPartInfo = tableMeta.getPartitionInfo();
                        isSingleTbOrBroadcastTb = firstPartInfo.getTableType() == PartitionTableType.SINGLE_TABLE
                            || firstPartInfo.getTableType() == PartitionTableType.BROADCAST_TABLE
                            || firstPartInfo.getTableType() == PartitionTableType.GSI_SINGLE_TABLE
                            || firstPartInfo.getTableType() == PartitionTableType.GSI_BROADCAST_TABLE;

                        partCnt = tableMeta.getPartitionInfo().getPartitionBy().getPartitions().size();
                        if (!isSingleTbOrBroadcastTb) {
                            List<List<ColumnMeta>> allLevelMaxActPartColMeta =
                                PartitionInfoUtil.getAllLevelMaxPartColumnMetasInfoForTableGroup(schemaName, tgName);

                            List<List<ColumnMeta>> allLevelActualPartColMeta =
                                PartitionInfoUtil.getAllLevelActualPartColumnMetasInfoForTableGroup(schemaName, tgName);

                            actualPartStr =
                                tableMeta.getPartitionInfo().getPartitionBy()
                                    .normalizePartitionKeyIgnoreColName(allLevelActualPartColMeta.get(0).size());
                            maxPartStr =
                                tableMeta.getPartitionInfo().getPartitionBy()
                                    .normalizePartitionKeyIgnoreColName(allLevelMaxActPartColMeta.get(0).size());

                            PartitionByDefinition subPartByDef =
                                tableMeta.getPartitionInfo().getPartitionBy().getSubPartitionBy();
                            boolean useSubPartTemp = subPartByDef != null;
                            if (useSubPartTemp) {
                                subPartCnt = tableMeta.getPartitionInfo().getAllPhysicalPartitionCount();
                                subPartTempStr = String.valueOf(subPartByDef.isUseSubPartTemplate());
                                actualSubPartStr = subPartByDef.normalizePartitionKeyIgnoreColName(
                                    allLevelActualPartColMeta.get(1).size());
                                maxSubPartStr = subPartByDef.normalizePartitionKeyIgnoreColName(
                                    allLevelMaxActPartColMeta.get(1).size());
                            } else {
                                subPartTempStr = "FALSE";
                                actualSubPartStr = "NO_PART_KEY";
                                maxSubPartStr = "NO_PART_KEY";
                            }
                        } else {
                            actualPartStr = "NO_PART_KEY";
                            maxPartStr = "NO_PART_KEY";
                            subPartTempStr = "FALSE";
                            actualSubPartStr = "NO_PART_KEY";
                            maxSubPartStr = "NO_PART_KEY";
                        }
                    }

                    cursor.addRow(new Object[] {
                        DataTypes.StringType.convertFrom(schemaName),
                        DataTypes.LongType.convertFrom(entry.getKey()),
                        DataTypes.StringType.convertFrom(tableGroupRecord.tg_name),
                        DataTypes.StringType.convertFrom(tableGroupRecord.locality),
                        DataTypes.StringType.convertFrom(tableGroupRecord.primary_zone),
                        DataTypes.IntegerType.convertFrom(tableGroupRecord.manual_create),

                        DataTypes.StringType.convertFrom(actualPartStr),
                        DataTypes.StringType.convertFrom(maxPartStr),
                        DataTypes.LongType.convertFrom(partCnt),

                        DataTypes.StringType.convertFrom(actualSubPartStr),
                        DataTypes.StringType.convertFrom(maxSubPartStr),
                        DataTypes.StringType.convertFrom(subPartTempStr),
                        DataTypes.LongType.convertFrom(subPartCnt),

                        DataTypes.LongType.convertFrom(logTblCnt),
                        DataTypes.LongType.convertFrom(idxTblCnt),
                    });
                } else {
                    if (tableGroupConfig.getTableCount() > 0) {
                        int tableCount = 0;
                        for (TablePartRecordInfoContext context : tableGroupConfig
                            .getAllTables()) {
                            String tableName = context.getLogTbRec().tableName;
                            TableMeta tableMeta = schemaManager.getTable(tableName);
                            if (tableCount == 0) {
                                try {
                                    PartitionInfo partInfo = tableMeta.getPartitionInfo();
                                    List<Integer> allLevelActualPartColCnts = partInfo.getAllLevelActualPartColCounts();
                                    partitionString =
                                        tableMeta.getPartitionInfo().getPartitionBy()
                                            .normalizePartitionByDefForShowTableGroup(tableGroupConfig, true,
                                                allLevelActualPartColCnts);
                                } catch (Throwable t) {
                                    logger.warn(t);
                                    continue;
                                }
                            } else {
                                sb.append(",");
                            }
                            if (tableMeta.isGsi()) {
                                String primaryTable = tableMeta.getGsiTableMetaBean().gsiMetaBean.tableName;
                                String unwrapGsiName = TddlSqlToRelConverter.unwrapGsiName(tableName);
                                tableName = String.format(LOGICAL_GSI_NAME, primaryTable, unwrapGsiName);
                            }
                            sb.append(tableName);
                            tableCount++;
                        }
                    }
                    cursor.addRow(new Object[] {
                        DataTypes.StringType.convertFrom(schemaName),
                        DataTypes.LongType.convertFrom(entry.getKey()),
                        DataTypes.StringType.convertFrom(tableGroupRecord.tg_name),
                        DataTypes.StringType.convertFrom(tableGroupRecord.locality),
                        DataTypes.StringType.convertFrom(tableGroupRecord.primary_zone),
                        DataTypes.IntegerType.convertFrom(tableGroupRecord.manual_create),
                        DataTypes.StringType.convertFrom(partitionString),
                        DataTypes.StringType.convertFrom(sb.toString())
                    });
                }

            }
        }

        return cursor;
    }
}


