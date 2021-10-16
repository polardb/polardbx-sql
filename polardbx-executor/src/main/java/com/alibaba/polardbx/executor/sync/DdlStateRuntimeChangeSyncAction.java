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

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.collections.CollectionUtils;

import java.util.Set;

/**
 * For PolarDB-X only.
 */
public class DdlStateRuntimeChangeSyncAction implements ISyncAction {
    protected final static Logger logger = LoggerFactory.getLogger(DdlStateRuntimeChangeSyncAction.class);

    private Set<Long> jobIdSet;
    private SqlKind sqlKind;

    public DdlStateRuntimeChangeSyncAction(Set<Long> jobIdSet,
                                           SqlKind sqlKind) {
        this.jobIdSet = jobIdSet;
        this.sqlKind = sqlKind;
    }

    @Override
    public ResultCursor sync() {
        if (CollectionUtils.isEmpty(jobIdSet)) {
            return null;
        }
        switch (sqlKind) {
        case ROLLBACK_DDL_JOB:
        case CANCEL_DDL_JOB:
            for (long jobId : jobIdSet) {
            }
            break;
        case PAUSE_DDL_JOB:
            for (long jobId : jobIdSet) {
            }
            break;
        default:
            break;
        }
        return null;
    }

    public Set<Long> getJobIdSet() {
        return this.jobIdSet;
    }

    public void setJobIdSet(final Set<Long> jobIdSet) {
        this.jobIdSet = jobIdSet;
    }

    public SqlKind getSqlKind() {
        return this.sqlKind;
    }

    public void setSqlKind(final SqlKind sqlKind) {
        this.sqlKind = sqlKind;
    }
}
