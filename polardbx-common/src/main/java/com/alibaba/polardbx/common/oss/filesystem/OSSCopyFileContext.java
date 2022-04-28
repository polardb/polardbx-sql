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

package com.alibaba.polardbx.common.oss.filesystem;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Used by OSSFileSystem and {@link OSSCopyFileTask}
 * as copy context. It contains some variables used in copy process.
 */
public class OSSCopyFileContext {
    private final ReentrantLock lock = new ReentrantLock();

    private Condition readyCondition = lock.newCondition();

    private boolean copyFailure;
    private int copiesFinish;

    public OSSCopyFileContext() {
        copyFailure = false;
        copiesFinish = 0;
    }

    public void lock() {
        lock.lock();
    }

    public void unlock() {
        lock.unlock();
    }

    public void awaitAllFinish(int copiesToFinish) throws InterruptedException {
        while (this.copiesFinish != copiesToFinish) {
            readyCondition.await();
        }
    }

    public void signalAll() {
        readyCondition.signalAll();
    }

    public boolean isCopyFailure() {
        return copyFailure;
    }

    public void setCopyFailure(boolean copyFailure) {
        this.copyFailure = copyFailure;
    }

    public void incCopiesFinish() {
        ++copiesFinish;
    }
}
