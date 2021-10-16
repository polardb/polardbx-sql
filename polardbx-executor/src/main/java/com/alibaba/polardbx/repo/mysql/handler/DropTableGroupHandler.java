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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.TableGroupSyncAction;
import com.alibaba.polardbx.gms.partition.TablePartitionAccessor;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupAccessor;
import com.alibaba.polardbx.gms.tablegroup.TableGroupAccessor;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropTableGroup;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import org.apache.calcite.rel.RelNode;

import java.sql.Connection;
import java.util.List;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class DropTableGroupHandler extends HandlerCommon {

    public static Logger logger = LoggerFactory.getLogger(DropTableGroupHandler.class);

    public DropTableGroupHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        LogicalDropTableGroup logicalDropTableGroup = (LogicalDropTableGroup) logicalPlan;
        String schemaName = logicalDropTableGroup.getSchemaName();
        if (schemaName == null) {
            schemaName = executionContext.getSchemaName();
        }
        String tableGroupName = logicalDropTableGroup.getTableGroupName();
        boolean isIfExists = logicalDropTableGroup.isIfExists();
        TableGroupInfoManager tableGroupInfoManager =
            OptimizerContext.getContext(schemaName).getTableGroupInfoManager();
        TableGroupConfig tableGroupConfig = tableGroupInfoManager.getTableGroupConfigByName(tableGroupName);
        if (tableGroupConfig != null && !GeneralUtil.isEmpty(tableGroupConfig.getAllTables())) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                String.format("The tablegroup[%s] is not empty, can't drop it", tableGroupName));
        } else {
            TableGroupAccessor tableGroupAccessor = new TableGroupAccessor();
            PartitionGroupAccessor partitionGroupAccessor = new PartitionGroupAccessor();
            TablePartitionAccessor tablePartitionAccessor = new TablePartitionAccessor();
            try (Connection connection = MetaDbUtil.getConnection()) {
                connection.setAutoCommit(false);
                tableGroupAccessor.setConnection(connection);
                partitionGroupAccessor.setConnection(connection);
                tablePartitionAccessor.setConnection(connection);
                List<TableGroupRecord>
                    tableGroupRecordList = tableGroupAccessor.getTableGroupsBySchemaAndName(schemaName, tableGroupName);
                if (GeneralUtil.isEmpty(tableGroupRecordList)) {
                    if (!isIfExists) {
                        throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                            String.format("drop tablegroup error, tablegroup[%s] is not exist", tableGroupName));
                    } else {
                        return new AffectRowCursor(new int[] {1});
                    }
                }
                assert tableGroupRecordList.size() == 1;
                Long tableGroupId = tableGroupRecordList.get(0).id;
                List<TablePartitionRecord> tablePartitionRecords =
                    tablePartitionAccessor.getTablePartitionsByDbNameGroupId(schemaName, tableGroupId);
                if (!GeneralUtil.isEmpty(tablePartitionRecords)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        String.format("The tablegroup[%s] is not empty, can't drop it", tableGroupName));
                }
                tableGroupAccessor.deleteTableGroupsById(schemaName, tableGroupId);
                partitionGroupAccessor.deletePartitionGroupsByTableGroupId(tableGroupId, false);
                connection.commit();
            } catch (Throwable ex) {
                MetaDbLogUtil.META_DB_LOG.error(ex);
                throw GeneralUtil.nestedException(ex);
            }
            SyncManagerHelper.sync(new TableGroupSyncAction(schemaName, tableGroupName), schemaName);
        }

        return new AffectRowCursor(new int[] {1});
    }
}
