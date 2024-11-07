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

import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.utils.PartitionMetaUtil;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.DbInfoRecord;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.common.PartitionTableType;
import com.alibaba.polardbx.optimizer.sql.sql2rel.TddlSqlToRelConverter;
import com.alibaba.polardbx.optimizer.view.InformationSchemaPartitionsMeta;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * @author chenghui.lch
 */
public class InformationSchemaPartitionsMetaHandler extends BaseVirtualViewSubClassHandler {

    private static final Logger logger = LoggerFactory.getLogger(InformationSchemaPartitionsMetaHandler.class);

    public InformationSchemaPartitionsMetaHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaPartitionsMeta;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        //InformationSchemaPartitionsMeta partitionsMeta = (InformationSchemaPartitionsMeta) virtualView;
        List<DbInfoRecord> allDbInfos = DbInfoManager.getInstance().getDbInfoList();
        Set<String> newPartDbNameSet = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < allDbInfos.size(); i++) {
            DbInfoRecord dbInfo = allDbInfos.get(i);
            if (DbInfoManager.getInstance().isNewPartitionDb(dbInfo.dbName)) {
                newPartDbNameSet.add(dbInfo.dbName);
            }
        }
        Iterator<String> dbItor = newPartDbNameSet.iterator();
        while (dbItor.hasNext()) {
            String dbName = dbItor.next();

            OptimizerContext oc = OptimizerContext.getContext(dbName);
            if (oc == null) {
                // maybe db init failed
                continue;
            }
            SchemaManager latestSm = oc.getLatestSchemaManager();
            PartitionInfoManager partInfoMgr = latestSm.getTddlRuleManager().getPartitionInfoManager();

            Set<String> newPartTbNameSet = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
            newPartTbNameSet.addAll(partInfoMgr.getPartitionTables());

            Map<String, String> gsiNameToPrimTblMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);

            Iterator<String> tbItor = newPartTbNameSet.iterator();
            while (tbItor.hasNext()) {
                String tbName = tbItor.next();
                PartitionInfo partInfo = partInfoMgr.getPartitionInfo(tbName);
                if (partInfo != null) {
                    if (partInfo.getTableType() == PartitionTableType.PARTITION_TABLE ||
                        partInfo.getTableType() == PartitionTableType.SINGLE_TABLE ||
                        partInfo.getTableType() == PartitionTableType.BROADCAST_TABLE
                    ) {
                        TableMeta tblMeta = latestSm.getTable(tbName);
                        tblMeta.getGsiPublished();
                        if (tblMeta.getGsiPublished() != null) {
                            Set<String> gsiNameSet = tblMeta.getGsiPublished().keySet();
                            Iterator<String> itor = gsiNameSet.iterator();
                            while (itor.hasNext()) {
                                String gsiName = itor.next();
                                gsiNameToPrimTblMap.put(gsiName, tbName);
                            }
                        }
                    }
                }
            }

            tbItor = newPartTbNameSet.iterator();
            while (tbItor.hasNext()) {
                String tbName = tbItor.next();
                PartitionInfo partInfo = partInfoMgr.getPartitionInfo(tbName);
                if (partInfo != null) {
                    String primTblName = null;
                    String rawGsiName = "";
                    if (partInfo.getTableType() == PartitionTableType.GSI_TABLE ||
                        partInfo.getTableType() == PartitionTableType.GSI_BROADCAST_TABLE ||
                        partInfo.getTableType() == PartitionTableType.GSI_SINGLE_TABLE
                    ) {
                        String gsiName = tbName;
                        primTblName = gsiNameToPrimTblMap.get(gsiName);
                        rawGsiName = TddlSqlToRelConverter.unwrapGsiName(gsiName);
                    } else {
                        primTblName = partInfo.getTableName();
                    }
                    try {
                        handlePartitionsMeta(cursor, partInfo, rawGsiName, primTblName);
                    } catch (Throwable ex) {
                        /**
                         * when some ddl of all databases run concurrently(such as ddl qa),
                         * maybe throw some npe, so
                         * ignore fetch the partMeta of the exception table
                         *
                         */
                        logger.warn("maybe some partition meta of tables have been changed", ex);
                    }
                }
            }
        }
        return cursor;
    }

    public static Cursor handlePartitionsMeta(ArrayResultCursor result,
                                              PartitionInfo partInfo,
                                              String rawGsiName,
                                              String primaryTbl) {
        List<PartitionMetaUtil.PartitionMetaRecord> partitionMetaRecords =
            PartitionMetaUtil.handlePartitionsMeta(partInfo, rawGsiName, primaryTbl);
        for (int i = 0; i < partitionMetaRecords.size(); i++) {
            PartitionMetaUtil.PartitionMetaRecord record = partitionMetaRecords.get(i);
            result.addRow(new Object[] {
                record.partNum,

                record.tableSchema,
                record.tableName,
                record.indexName,
                record.primaryTable,
                record.tableType,
                record.tgName,

                record.partMethod,
                record.partCol,
                record.partColType,
                record.partExpr,
                record.partName,
                record.partPosi,
                record.partDesc,
                record.partArcStateName,

                record.subPartMethod,
                record.subPartCol,
                record.subPartColType,
                record.subPartExpr,
                record.subPartName,
                record.subPartTempName,
                record.subPartPosi,
                record.subPartDesc,
                record.subPartArcStateName,

                record.pgName,
                record.phyDbGroup,
                record.phyDb,
                record.phyTb,
                record.rwDnId

            });
        }
        return result;
    }

}
