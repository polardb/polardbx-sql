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

import com.alibaba.polardbx.common.logger.LoggerInit;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.optimizer.sequence.SequenceManagerProxy;

public class ClearSeqCacheSyncAction implements ISyncAction {

    private String schemaName;
    private String seqName;
    private boolean all;
    private boolean reset;

    public ClearSeqCacheSyncAction() {
    }

    public ClearSeqCacheSyncAction(String schemaName, String seqName, boolean all, boolean reset) {
        this.schemaName = schemaName;
        this.seqName = seqName;
        this.all = all;
        this.reset = reset;
    }

    @Override
    public ResultCursor sync() {
        if (all) {
            SequenceManagerProxy.getInstance().invalidateAll(schemaName);
            LoggerInit.TDDL_SEQUENCE_LOG.info("All sequence caches have been cleared");
            if (reset) {
                SequenceManagerProxy.getInstance().resetNewSeqResources(schemaName);
                LoggerInit.TDDL_SEQUENCE_LOG.info("New Sequence's queues and handlers have been reset");
            }
        } else {
            SequenceManagerProxy.getInstance().invalidate(schemaName, seqName);
            LoggerInit.TDDL_SEQUENCE_LOG.info("The sequence cache for '" + seqName + "' has been cleared");
        }
        return null;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getSeqName() {
        return seqName;
    }

    public void setSeqName(String seqName) {
        this.seqName = seqName;
    }

    public boolean isAll() {
        return all;
    }

    public void setAll(boolean all) {
        this.all = all;
    }

    public boolean isReset() {
        return reset;
    }

    public void setReset(boolean reset) {
        this.reset = reset;
    }
}
