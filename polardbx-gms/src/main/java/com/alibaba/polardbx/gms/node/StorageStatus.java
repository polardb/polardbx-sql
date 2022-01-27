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
