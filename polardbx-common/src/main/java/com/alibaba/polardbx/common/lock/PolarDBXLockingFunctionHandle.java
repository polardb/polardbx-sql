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

package com.alibaba.polardbx.common.lock;

import com.alibaba.polardbx.common.logical.ITConnection;

public class PolarDBXLockingFunctionHandle extends LockingFunctionHandle {
    PolarDBXLockingFunctionHandle(LockingFunctionManager manager, ITConnection tddlConnection, String drdsSession) {
        super(manager, tddlConnection, drdsSession);
    }

    @Override
    public Integer tryAcquireLock(String lockName, int timeout) {
        return 0;
    }

    @Override
    public Integer release(String lockName) {
        return 0;
    }

    @Override
    public Integer releaseAllLocks() {
        return 0;
    }

    @Override
    public Integer isFreeLock(String lockName) {
        return 0;
    }

    @Override
    public String isUsedLock(String lockName) {
        return null;
    }
}
