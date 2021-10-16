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

package org.apache.calcite.sql;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;

/**
 * Utility of ddl job: continue, recover, rollback ...
 *
 * @author moyi
 * @since 2021/07
 */
public interface DdlJobUtility {

    /**
     * Whether RECOVER/ROLLBACK ALL
     */
    boolean isAll();

    /**
     * Get job ids of command
     */
    List<Long> getJobIds();

    /**
     * Check whether job ids are empty
     */
    default void checkJobIdsEmpty() {
        if (!isAll() && CollectionUtils.isEmpty(getJobIds())) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_UNEXPECTED, "no job id provided");
        }
    }
}
