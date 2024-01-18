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

package com.alibaba.polardbx.stats;

import com.alibaba.polardbx.common.type.TransactionType;
import io.airlift.slice.XxHash64;

/**
 * Distributed transaction metrics
 */
public class TransactionStatistics {
    public TransactionType transactionType;
    public boolean readOnly = true;
    public long startTimeInMs = 0;
    public long startTime = 0;
    public long sqlStartTime = 0;
    public long sqlFinishTime = 0;
    public long durationTime = 0;
    public long finishTimeInMs = 0;
    public long activeTime = 0;
    public long idleTime = 0;
    public long mdlWaitTime = 0;
    public long getTsoTime = 0;
    public long prepareTime = 0;
    public long commitTime = 0;
    public long trxLogTime = 0;
    public long rollbackTime = 0;
    public long readTime = 0;
    public long readReturnRows = 0;
    public long writeTime = 0;
    public long writeAffectRows = 0;
    public long sqlCount = 0;
    public long rwSqlCount = 0;
    public long lastActiveTime = 0;
    public XxHash64 trxTemplate = new XxHash64();

    public enum Status {
        UNKNOWN, COMMIT, ROLLBACK, COMMIT_FAIL, ROLLBACK_FAIL, KILL
    }

    public Status status = Status.UNKNOWN;

    public void setIfUnknown(Status s) {
        if (Status.UNKNOWN == status) {
            status = s;
        }
    }
}
