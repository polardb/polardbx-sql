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

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.gms.partition.TablePartRecordInfoContext;
import com.alibaba.polardbx.gms.tablegroup.TableGroupAccessor;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import com.alibaba.polardbx.optimizer.view.InformationSchemaTableGroup;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class InformationSchemaTableGroupHandler extends BaseVirtualViewSubClassHandler{
    public InformationSchemaTableGroupHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaTableGroup;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        TableGroupAccessor tableGroupAccessor = new TableGroupAccessor();
        try (Connection connection = MetaDbUtil.getConnection()) {
            tableGroupAccessor.setConnection(connection);
            final List<String> schemaNames = tableGroupAccessor.getDistinctSchemaNames();
            for (String schemaName : schemaNames) {
                TableGroupInfoManager tableGroupInfoManager =
                    OptimizerContext.getContext(schemaName).getTableGroupInfoManager();
                SchemaManager schemaManager = executionContext.getSchemaManager(schemaName);
                final Map<Long, TableGroupConfig> tableGroupConfigMap =
                    tableGroupInfoManager.getTableGroupConfigInfoCache();
                if (tableGroupConfigMap != null) {
                    for (Map.Entry<Long, TableGroupConfig> entry : tableGroupConfigMap.entrySet()) {
                        StringBuilder sb = new StringBuilder();
                        TableGroupConfig tableGroupConfig = entry.getValue();
                        TableGroupRecord tableGroupRecord = tableGroupConfig.getTableGroupRecord();
                        String partitionString = "";
                        if (tableGroupConfig.getTableCount() > 0) {
                            int tableCount = 0;
                            for (TablePartRecordInfoContext context : tableGroupConfig
                                .getAllTables()) {
                                String tableName = context.getLogTbRec().tableName;
                                if (tableCount == 0) {
                                    TableMeta tableMeta = schemaManager.getTable(context.getLogTbRec().tableName);
                                    partitionString =
                                        tableMeta.getPartitionInfo().getPartitionBy()
                                            .normalizePartitionByInfo(tableGroupConfig, true);
                                } else {
                                    sb.append(",");
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
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
        return cursor;
    }
}

