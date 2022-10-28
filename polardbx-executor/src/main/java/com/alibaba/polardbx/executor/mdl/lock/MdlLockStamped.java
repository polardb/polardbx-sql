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

package com.alibaba.polardbx.executor.mdl.lock;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.mdl.MdlKey;
import com.alibaba.polardbx.executor.mdl.MdlLock;

import com.alibaba.polardbx.executor.mpp.metadata.NotNull;
import java.util.concurrent.locks.StampedLock;

/**
 * @author chenmo.cm
 */
public class MdlLockStamped extends MdlLock {

    protected final StampedLock stampedLock;

    public MdlLockStamped(@NotNull MdlKey key) {
        super(key);
        stampedLock = new StampedLock();
    }

    @Override
    public long writeLock() {
        return stampedLock.writeLock();
    }

    @Override
    public long readLock() {
        try {
            return stampedLock.readLockInterruptibly();
        } catch (InterruptedException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "mdl readlock interrupted", e);
        }
    }

    @Override
    public void unlockWrite(long stamp) {
        stampedLock.unlockWrite(stamp);
    }

    @Override
    public void unlockRead(long stamp) {
        stampedLock.unlockRead(stamp);
    }

    @Override
    public boolean isLocked() {
        return stampedLock.isReadLocked() || stampedLock.isWriteLocked();
    }

    @Override
    public String toString() {
        return "MdlLockStamped{" + "stampedLock=" + stampedLock + '}';
    }
}
