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

import static org.apache.calcite.sql.SqlKind.FLASHBACK_TABLE;
import static org.apache.calcite.sql.SqlKind.PURGE;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalRecyclebin;
import org.apache.calcite.sql.SqlFlashbackTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPurge;
import org.apache.calcite.util.Util;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.executor.common.RecycleBin;
import com.alibaba.polardbx.executor.common.RecycleBinManager;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;

/**
 * @author desai
 */
public class LogicalRecyclebinHandler extends HandlerCommon {

    public LogicalRecyclebinHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        RecycleBin bin = RecycleBinManager.instance.getByAppName(executionContext.getAppName());
        if (bin == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_RECYCLEBIN_EXECUTE, "can't find recycle bin");
        }

        LogicalRecyclebin dummy = (LogicalRecyclebin) logicalPlan;
        SqlNode node = dummy.getNode();
        if (node.getKind() == FLASHBACK_TABLE) {
            SqlFlashbackTable flashback = (SqlFlashbackTable) node;
            String name = Util.last(flashback.getName().names);
            String renameTo = flashback.getRenameTo() == null ? null : Util.last(flashback.getRenameTo().names);
            bin.flashback(name, renameTo);
        }

        if (node.getKind() == PURGE) {
            SqlPurge purge = (SqlPurge) node;
            if (purge.getName() != null) {
                bin.purgeTable(Util.last(purge.getName().names), true);
            } else {
                bin.purge(false);
            }
        }
        return new AffectRowCursor(new int[] {0});
    }
}
