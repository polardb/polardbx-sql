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

package com.alibaba.polardbx.common.model.lifecycle;

import com.alibaba.polardbx.common.utils.GeneralUtil;

public class AbstractLifecycle implements Lifecycle {

    protected final Object     lock       = new Object();
    protected volatile boolean isInited   = false;

    protected boolean          useTryLock = false;

    private volatile boolean   locked     = false;
    private volatile boolean   available  = true;

    private Throwable          lastError  = null;

    @Override
    public void init() {
        boolean needReleaseLock = checkAvailableAndGetLock();
        synchronized (lock) {
            try {
                if (isInited()) {
                    return;
                }

                try {
                    doInit();
                    isInited = true;
                    this.available = true;
                } catch (Throwable e) {
                    lastError = e;
                    this.available = false;

                    try {
                        doDestroy();
                    } catch (Exception e1) {

                    }

                    throw GeneralUtil.nestedException(e);
                }

            } finally {
                if (needReleaseLock) {
                    clearLock();
                }
            }

        }

    }

    @Override
    public void destroy() {
        synchronized (lock) {
            if (!isInited()) {
                return;
            }

            doDestroy();
            isInited = false;
        }
    }

    @Override
    public boolean isInited() {
        return isInited;
    }

    protected void doInit() {
    }

    protected void doDestroy() {
    }

    boolean isLocked() {
        return locked;
    }

    boolean checkAvailableAndGetLock() {
        if (!this.useTryLock) {
            return false;
        }

        if (this.available) {
            return false;
        }

        if (locked) {
            throw GeneralUtil.nestedException(getNotAvailableErrorMsg(), lastError);
        }

        synchronized (this) {

            if (locked) {
                throw GeneralUtil.nestedException(getNotAvailableErrorMsg(), lastError);
            }
            locked = true;
            return true;
        }
    }

    void clearLock() {
        this.locked = false;
    }

    protected String getNotAvailableErrorMsg() {
        return this.getClass().getSimpleName() + " is not available, please check and try again later.";
    }

    public Throwable getLastError() {
        return lastError;
    }

}
