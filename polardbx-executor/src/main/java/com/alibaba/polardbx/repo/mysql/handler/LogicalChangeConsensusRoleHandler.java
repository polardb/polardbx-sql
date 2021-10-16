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
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.ha.impl.StorageRole;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalChangeConsensusLeader;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlChangeConsensusRole;

/**
 * @author moyi
 * @since 2021/03
 */
public class LogicalChangeConsensusRoleHandler extends HandlerCommon {

    public LogicalChangeConsensusRoleHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalChangeConsensusLeader plan = (LogicalChangeConsensusLeader) logicalPlan;
        final SqlChangeConsensusRole sqlNode = (SqlChangeConsensusRole) plan.getNativeSqlNode();
        final StorageHaManager shm = StorageHaManager.getInstance();
        final String targetType = RelUtils.stringValue(sqlNode.getTargetType());
        final String target = RelUtils.stringValue(sqlNode.getTarget());
        final String roleStr = RelUtils.stringValue(sqlNode.getRole());
        final StorageRole role = StorageRole.getStorageRoleByString(roleStr);

        ArrayResultCursor result = new ArrayResultCursor("NEW LEADER");
        result.addColumn("NEW LEADER", DataTypes.StringType);
        result.initMeta();

        if ("node".equalsIgnoreCase(targetType)) {
            if (role == StorageRole.LOGGER) {
                throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT, String.format("change role to %s", role));
            }

            shm.changeRoleByAddress(target, role);
        } else if ("zone".equalsIgnoreCase(targetType)) {
            shm.changeRoleByZone(target, role);
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT,
                String.format("change type %s", targetType));
        }

        String message = String.format("change %s %s to %s", targetType, target, role);
        result.addRow(new Object[] {message});

        return result;
    }
}

