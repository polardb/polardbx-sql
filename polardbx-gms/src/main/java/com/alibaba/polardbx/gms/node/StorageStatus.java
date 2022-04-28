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

package com.alibaba.polardbx.gms.node;

import java.io.Serializable;

public class StorageStatus implements Serializable {

    private String instId;
    /**
     * <pre>
     *      Calculate the delay second for learner.
     *      CheckStorageHaTask at intervals of 3 seconds
     * </pre>
     */
    private long delaySecond = 0;
    /**
     * <pre>
     *      Calculate the active session num for learner.
     *      CheckStorageHaTask at intervals of 3 seconds
     * </pre>
     */
    private long activeSession = 0;
    private boolean isBusy;
    private boolean isDelay;

    public StorageStatus() {
    }

    public StorageStatus(String instId, long delaySecond, long activeSession, boolean isBusy, boolean isDelay) {
        this.instId = instId;
        this.delaySecond = delaySecond;
        this.activeSession = activeSession;
        this.isBusy = isBusy;
        this.isDelay = isDelay;
    }

    public long getDelaySecond() {
        return delaySecond;
    }

    public long getActiveSession() {
        return activeSession;
    }

    public boolean isBusy() {
        return isBusy;
    }

    public boolean isDelay() {
        return isDelay;
    }

    public void setDelaySecond(long delaySecond) {
        this.delaySecond = delaySecond;
    }

    public void setActiveSession(long activeSession) {
        this.activeSession = activeSession;
    }

    public void setBusy(boolean busy) {
        isBusy = busy;
    }

    public void setDelay(boolean delay) {
        isDelay = delay;
    }

    public String getInstId() {
        return instId;
    }

    public void setInstId(String instId) {
        this.instId = instId;
    }
}
