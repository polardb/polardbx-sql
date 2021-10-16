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

package com.alibaba.polardbx.executor.mdl;

/**
 * 持有 MDL 的时间, 目前仅使用 MDL_TRANSACTION
 */
public enum MdlDuration {
    /**
     * Locks with statement duration are automatically released at the end
     * of statement or transaction.
     */
    MDL_STATEMENT,
    /**
     * Locks with transaction duration are automatically released at the end
     * of transaction.
     */
    MDL_TRANSACTION,
    /**
     * Locks with explicit duration survive the end of statement and
     * transaction. They have to be released explicitly by calling
     * MDL_context::release_lock().
     */
    MDL_EXPLICIT;

    public boolean transactional() {
        return this == MDL_TRANSACTION || this == MDL_STATEMENT;
    }
}
