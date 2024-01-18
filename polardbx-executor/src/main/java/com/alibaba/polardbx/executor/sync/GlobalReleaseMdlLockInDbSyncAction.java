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

package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.mdl.MdlContext;
import com.alibaba.polardbx.executor.mdl.MdlManager;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import lombok.Getter;

import java.text.MessageFormat;
import java.util.Set;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
@Getter
public class GlobalReleaseMdlLockInDbSyncAction implements ISyncAction {
    final private Set<String> schemaNames;

    public GlobalReleaseMdlLockInDbSyncAction(Set<String> schemaNames) {
        this.schemaNames = schemaNames;
    }

    @Override
    public ResultCursor sync() {
        for (String schema : schemaNames) {
            releaseAllTablesMdlWriteLockInDb(schema);
        }
        return null;
    }

    protected void releaseAllTablesMdlWriteLockInDb(String schemaName) {
        SQLRecorderLogger.ddlLogger.info(MessageFormat.format(
            "[{0} {1}] Mdl write lock try to release all tables in schema[{2}]",
            Thread.currentThread().getName(), this.hashCode(), schemaName));

        final MdlContext mdlContext = MdlManager.addContext(schemaName, true);
        mdlContext.releaseAllTransactionalLocks();
        MdlManager.removeContext(mdlContext);

        SQLRecorderLogger.ddlLogger.info(MessageFormat.format(
            "[{0} {1}] Mdl write lock release all tables in schema[{2}]",
            Thread.currentThread().getName(), this.hashCode(), schemaName));
    }
}
