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

package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.atom.TAtomDataSource;
import com.alibaba.polardbx.atom.config.TAtomDsConfDO;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.model.Matrix;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IGroupExecutor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoAccessor;
import com.alibaba.polardbx.gms.topology.GroupDetailInfoExRecord;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.group.config.Weight;
import com.alibaba.polardbx.group.jdbc.DataSourceWrapper;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.locality.StoragePoolManager;
import org.apache.calcite.rel.RelNode;

import java.sql.Connection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @author chenmo.cm
 */
public class LogicalShowDsHandler extends HandlerCommon {
    public LogicalShowDsHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        return handleDsForPolarDBX(logicalPlan, executionContext);
    }

    public Cursor handleDsForPolarDBX(RelNode logicalPlan, ExecutionContext executionContext) {

        ArrayResultCursor result = new ArrayResultCursor("STORAGE_PHY_DB_INFO");
        result.addColumn("ID", DataTypes.IntegerType);
        result.addColumn("STORAGE_INST_ID", DataTypes.StringType);
        result.addColumn("DB", DataTypes.StringType);
        result.addColumn("GROUP", DataTypes.StringType);
        result.addColumn("PHY_DB", DataTypes.StringType);
        result.addColumn("MOVABLE", DataTypes.IntegerType);
        result.addColumn("STORAGE_POOL", DataTypes.StringType);

        int index = 0;
        try (Connection metaDbConn = MetaDbDataSource.getInstance().getConnection()) {

            StoragePoolManager storagePoolManager = StoragePoolManager.getInstance();
            GroupDetailInfoAccessor groupDetailInfoAccessor = new GroupDetailInfoAccessor();
            groupDetailInfoAccessor.setConnection(metaDbConn);
            List<GroupDetailInfoExRecord> completedGroupInfos =
                groupDetailInfoAccessor.getCompletedGroupInfosByInstId(InstIdUtil.getInstId());
            Collections.sort(completedGroupInfos);

            for (int i = 0; i < completedGroupInfos.size(); i++) {
                GroupDetailInfoExRecord groupDetailInfoExRecord = completedGroupInfos.get(i);
                String storageInstId = groupDetailInfoExRecord.storageInstId;
                String dbName = groupDetailInfoExRecord.dbName;
                String grpName = groupDetailInfoExRecord.groupName;
                String phyDbName = groupDetailInfoExRecord.phyDbName;
                String storagePool = storagePoolManager.storagePoolMap.getOrDefault(storageInstId, "");
                boolean movable = !GroupInfoUtil.isSingleGroup(grpName);
                result.addRow(new Object[] {
                    index++, storageInstId, dbName, grpName,
                    phyDbName, movable ? 1 : 0, storagePool});
            }

        } catch (Throwable ex) {
            throw GeneralUtil.nestedException("Failed to get storage and phy db info", ex);
        }

        return result;
    }

}
