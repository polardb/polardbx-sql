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

import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.optimizer.sequence.SequenceManagerProxy;

/**
 * @author agapple 2015/3/26 20:07:41
 * @since 5.1.19
 */
public class SequenceSyncAction implements ISyncAction {

    private String sequenceName;
    private String schemaName;

    public SequenceSyncAction() {

    }

    public SequenceSyncAction(String schemaName, String sequenceName) {
        this.schemaName = schemaName;
        this.sequenceName = sequenceName;
    }

    @Override
    public ResultCursor sync() {
        if (!ConfigDataMode.isMasterMode()) {
            return null;
        }
        // After sequence DDLs, we should invalidate cached sequence,
        // so that subsequent request can get the latest information.
        SequenceManagerProxy.getInstance().invalidate(schemaName, sequenceName);
        return null;
    }

    public String getSequenceName() {
        return sequenceName;
    }

    public void setSequenceName(String sequenceName) {
        this.sequenceName = sequenceName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }
}
