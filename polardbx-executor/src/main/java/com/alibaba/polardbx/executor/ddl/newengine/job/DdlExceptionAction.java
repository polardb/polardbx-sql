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

package com.alibaba.polardbx.executor.ddl.newengine.job;

import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.executor.ddl.newengine.utils.TaskHelper;

/**
 * don't delete or modify any value of this enum if you're not 100% sure what you are doing
 *
 * @see TaskHelper#fromDdlEngineTaskRecord(com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskRecord)
 * @see DdlState
 */
public enum DdlExceptionAction {

    /**
     * try to recover the job when failed (n times)
     * DdlState will turn to PAUSED if fail to recover
     */
    TRY_RECOVERY_THEN_PAUSE,
    /**
     * try to rollback the job
     * DdlState will turn to FAILED_ROLLBACK if fail to recover
     */
    ROLLBACK,
    /**
     * try to recover first
     * if failed, then try to rollback
     */
    TRY_RECOVERY_THEN_ROLLBACK,
    /**
     * DdlState will turn to PAUSED
     */
    PAUSE;

    public static DdlExceptionAction DEFAULT_ACTION = TRY_RECOVERY_THEN_PAUSE;

    public static boolean isRollbackAble(DdlExceptionAction ddlExceptionAction) {
        return ddlExceptionAction == ROLLBACK || ddlExceptionAction == TRY_RECOVERY_THEN_ROLLBACK;
    }

}
