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

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.locality.PrimaryZoneInfo;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterSystemSetConfig;
import com.alibaba.polardbx.optimizer.locality.LocalityManager;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlAlterSystemSetConfig;

public class LogicalAlterSystemSetConfigHandler extends HandlerCommon {

    public LogicalAlterSystemSetConfigHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final SqlAlterSystemSetConfig sqlNode =
            (SqlAlterSystemSetConfig) ((LogicalAlterSystemSetConfig) logicalPlan).getNativeSqlNode();
        final StorageHaManager shm = StorageHaManager.getInstance();
        final String primaryZone = sqlNode.getPrimaryZone();
        final LocalityManager lm = LocalityManager.getInstance();
        final PrimaryZoneInfo currentPrimaryZone = LocalityManager.getInstance().getSystemPrimaryZone();

        ArrayResultCursor result = new ArrayResultCursor("PRIMARY_ZONE");
        result.addColumn("PRIMARY_ZONE", DataTypes.StringType);
        result.initMeta();

        // validate
        PrimaryZoneInfo primaryZoneInfo = PrimaryZoneInfo.parse(primaryZone);
        shm.changePrimaryZone(primaryZoneInfo);

        // store in metadb
        lm.setSystemPrimaryZone(primaryZoneInfo);

        String message = String.format("change primary_zone from %s to %s", currentPrimaryZone, primaryZone);
        result.addRow(new Object[] {message});

        return result;

    }
}
