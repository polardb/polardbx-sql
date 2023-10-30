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
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.balancer.stats.StatsUtils;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.PartitionMetaUtil;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.filter.Like;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.common.PartitionTableType;
import com.alibaba.polardbx.optimizer.view.InformationSchemaPartitions;
import com.alibaba.polardbx.optimizer.view.InformationSchemaTableDetail;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * @author chenghui.lch
 */
public class InformationSchemaPartitionsHandler extends BaseVirtualViewSubClassHandler {

    public InformationSchemaPartitionsHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaPartitions;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {

        InformationSchemaPartitions informationSchemaPartitions = (InformationSchemaPartitions) virtualView;

        // only new partitioning db
        Set<String> schemaNames = new TreeSet<>(String::compareToIgnoreCase);
        schemaNames.addAll(StatsUtils.getDistinctSchemaNames());

        List<Object> tableSchemaIndexValue =
            virtualView.getIndex().get(informationSchemaPartitions.getTableSchemaIndex());

        Object tableSchemaLikeValue =
            virtualView.getLike().get(informationSchemaPartitions.getTableSchemaIndex());

        List<Object> tableNameIndexValue =
            virtualView.getIndex().get(informationSchemaPartitions.getTableNameIndex());

        Object tableNameLikeValue =
            virtualView.getLike().get(informationSchemaPartitions.getTableNameIndex());

        Map<Integer, ParameterContext> params = executionContext.getParams().getCurrentParameter();

        // schemaIndex
        Set<String> indexSchemaNames = new HashSet<>();
        if (tableSchemaIndexValue != null && !tableSchemaIndexValue.isEmpty()) {
            for (Object obj : tableSchemaIndexValue) {
                ExecUtils.handleTableNameParams(obj, params, indexSchemaNames);
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
                schemaNames = schemaNames.stream().filter(schemaName -> new Like().like(schemaName, likeArg)).collect(
                    Collectors.toSet());
            }
        }

        // tableIndex
        Set<String> indexTableNames = new HashSet<>();
        if (tableNameIndexValue != null && !tableNameIndexValue.isEmpty()) {
            for (Object obj : tableNameIndexValue) {
                ExecUtils.handleTableNameParams(obj, params, indexTableNames);
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

        handlePartitionsStat(schemaNames, indexTableNames, tableLike, executionContext, cursor);
        return cursor;
    }

    protected void handlePartitionsStat(
        Set<String> schemaNames,
        Set<String> logicalTableNames,
        String tableLike,
        ExecutionContext executionContext,
        ArrayResultCursor cursor
    ) {

        // get all phy tables(partitions) info from all DNs
        Map<String/**phyDbName**/, Pair<String/**storageInstId**/, String/**groupName**/>> storageInstIdGroupNames =
            new HashMap<>();
        /**
         *
         0: PART_DESC
         1: LOGICAL_TABLE_NAME
         2: PHYSICAL_TABLE
         3: PHYSICAL_SCHEMA
         4: TABLE_ROWS
         5: DATA_LENGTH
         6: INDEX_LENGTH
         7: ROWS_READ
         8: ROWS_INSERTED
         9: ROWS_UPDATED
         10: ROWS_DELETED
         *
         */
        Map<String/** dbName **/, Map<String, List<Object>> /** phy tables **/> phyDbTablesInfo =
            StatsUtils.queryTableSchemaStats(schemaNames, logicalTableNames, tableLike, storageInstIdGroupNames, null);

        for (String schemaName : schemaNames) {
            if (!DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
                continue;
            }
            SchemaManager sm = executionContext.getSchemaManager(schemaName);
            List<PartitionInfo> allPartInfos = sm.getTddlRuleManager().getPartitionInfoManager().getPartitionInfos();
            for (int i = 0; i < allPartInfos.size(); i++) {
                PartitionInfo partInfo = allPartInfos.get(i);
                if (partInfo.getTableType().isGsiTableType()) {
                    continue;
                }
                String tblName = partInfo.getTableName();
                if (logicalTableNames != null && !logicalTableNames.isEmpty() && !logicalTableNames.contains(
                    tblName.toLowerCase())) {
                    continue;
                }
                List<PartitionMetaUtil.PartitionMetaRecord> partMetaRecords =
                    PartitionMetaUtil.handlePartitionsMeta(partInfo, "", partInfo.getTableName());
                for (int j = 0; j < partMetaRecords.size(); j++) {

                    PartitionMetaUtil.PartitionMetaRecord metaRecord = partMetaRecords.get(j);

                    String phyDb = metaRecord.phyDb;
                    String phyTb = metaRecord.phyTb;

                    Map<String, List<Object>> phyTbStatMap = phyDbTablesInfo.get(phyDb.toLowerCase());
                    if (phyTbStatMap == null) {
                        continue;
                    }
                    List<Object> phyTbStat = phyTbStatMap.get(phyTb.toLowerCase());
                    if (phyTbStat != null) {
                        //
                        /**
                         *          4: TABLE_ROWS
                         *          5: DATA_LENGTH
                         *          6: INDEX_LENGTH
                         */
                        Long phyTblRows = DataTypes.LongType.convertFrom(phyTbStat.get(4));
                        Long phyTblDataLen = DataTypes.LongType.convertFrom(phyTbStat.get(5));
                        Long phyTblIndexLen = DataTypes.LongType.convertFrom(phyTbStat.get(6));

                        Long phyAvgRowsLen = 0L;
                        if (phyTblRows > 0) {
                            phyAvgRowsLen = phyTblDataLen / phyTblRows;
                        }

                        String tableCatalog = "def";

                        cursor.addRow(new Object[] {
                            tableCatalog,

                            metaRecord.tableSchema,
                            metaRecord.tableName,
                            metaRecord.partName,
                            metaRecord.subPartName,

                            metaRecord.partPosi,
                            metaRecord.subPartPosi,
                            metaRecord.partMethod,
                            metaRecord.subPartMethod,
                            metaRecord.partExpr,
                            metaRecord.subPartExpr,
                            metaRecord.partDesc,
                            metaRecord.subPartDesc,

                            phyTblRows,

                            phyAvgRowsLen,
                            phyTblDataLen,
                            null/*MAX_DATA_LENGTH*/,
                            phyTblIndexLen,
                            null/*DATA_FREE*/,

                            null/*CREATE_TIME*/,
                            null/*UPDATE_TIME*/,
                            null/*CHECK_TIME*/,
                            null/*CHECKSUM*/,

                            ""/*PARTITION_COMMENT*/,

                            "default"/*NODEGROUP*/,
                            null/*TABLESPACE_NAME*/

                        });
                    }

                    //
                }
            }
        }

        return;
    }

}
