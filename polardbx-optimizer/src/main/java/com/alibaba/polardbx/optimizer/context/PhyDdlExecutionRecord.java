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

package com.alibaba.polardbx.optimizer.context;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.encrypt.MD5Utils;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class PhyDdlExecutionRecord {

    public PhyDdlExecutionRecord(final long jobId, final long taskId, final int numPhyObjectsTotal) {
        this.jobId = jobId;
        this.taskId = taskId;
        this.numPhyObjectsTotal = numPhyObjectsTotal;
    }

    private long jobId;
    private long taskId;

    private AtomicBoolean hasInjected = new AtomicBoolean(false);

    private Set<String> phyObjectsDone = ConcurrentHashMap.newKeySet();

    private int numPhyObjectsTotal;
    private AtomicInteger numPhyObjectsDone = new AtomicInteger(0);

    private Set<String> errorHashesIgnored = null;

    public long getJobId() {
        return this.jobId;
    }

    public void setJobId(final long jobId) {
        this.jobId = jobId;
    }

    public long getTaskId() {
        return this.taskId;
    }

    public void setTaskId(final long taskId) {
        this.taskId = taskId;
    }

    public AtomicBoolean getHasInjected() {
        return this.hasInjected;
    }

    public Set<String> getPhyObjectsDone() {
        return phyObjectsDone;
    }

    public void setPhyObjectsDone(Set<String> phyObjectsDone) {
        this.phyObjectsDone = phyObjectsDone;
    }

    public void clearPhyObjectsDone() {
        this.phyObjectsDone.clear();
        this.numPhyObjectsDone.set(0);
    }

    public void addPhyObjectDone(String phyObjectDone) {
        this.phyObjectsDone.add(phyObjectDone);
    }

    public void removePhyObjectDone(String phyObjectDone) {
        this.phyObjectsDone.remove(phyObjectDone);
    }

    public int getNumPhyObjectsTotal() {
        return numPhyObjectsTotal;
    }

    public void setNumPhyObjectsTotal(int numPhyObjectsTotal) {
        this.numPhyObjectsTotal = numPhyObjectsTotal;
    }

    public int getNumPhyObjectsDone() {
        return numPhyObjectsDone.get();
    }

    public void setNumPhyObjectsDone(int numPhyObjectsDone) {
        this.numPhyObjectsDone.set(numPhyObjectsDone);
    }

    public void increasePhyObjsDone() {
        this.numPhyObjectsDone.incrementAndGet();
    }

    public void decreasePhyObjsDone() {
        this.numPhyObjectsDone.decrementAndGet();
    }

    public void addErrorIgnored(ExecutionContext.ErrorMessage errorMessage) {
        if (this.errorHashesIgnored == null) {
            synchronized (this) {
                if (this.errorHashesIgnored == null) {
                    this.errorHashesIgnored = ConcurrentHashMap.newKeySet();
                }
            }
        }
        String errorHashIgnored = MD5Utils.getInstance().getMD5String(errorMessage.getGroupName()
            + errorMessage.getCode()
            + errorMessage.getMessage());
        this.errorHashesIgnored.add(errorHashIgnored);
    }

    public Set<String> getErrorHashesIgnored() {
        return errorHashesIgnored;
    }

    public void setErrorHashesIgnored(Set<String> errorHashesIgnored) {
        this.errorHashesIgnored = errorHashesIgnored;
    }

    @Override
    public String toString() {
        StringBuilder info = new StringBuilder();

        info.append("Job ID: ").append(jobId).append("\n");
        info.append("Task ID: ").append(taskId).append("\n\n");

        info.append("Total Number of Physical Objects: ").append(numPhyObjectsTotal).append("\n");
        info.append("Number of Physical Objects Done: ").append(numPhyObjectsDone.get()).append("\n\n");

        info.append("Physical Objects Done:").append("\n");
        if (GeneralUtil.isNotEmpty(phyObjectsDone)) {
            phyObjectsDone.forEach(phyObjectDone -> info.append(phyObjectDone).append("\n"));
        }

        return info.toString();
    }

}
