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

package com.alibaba.polardbx.executor.utils.transaction;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Local transaction is a part of a global (distributed) transaction
 *
 * @author wuzhe
 */
public class LocalTransaction {
    // We assume that each group has one and only one local transaction under the same global transaction
    private String group;
    private String state;
    private String physicalSql;
    private String operationState;
    private Integer tablesInUse;
    private Integer tablesLocked;
    private Integer lockStructs;
    private Integer heapSize;
    private Integer rowLocks;
    private TrxLock waitingTrxLock;
    private Set<TrxLock> holdingTrxLocks;
    private boolean isUpdated;

    public LocalTransaction(String group) {
        this.group = group;
    }

    // ---------- getter methods ----------

    public String getGroup() {
        return group;
    }

    public String getState() {
        return state;
    }

    public String getPhysicalSql() {
        return physicalSql;
    }

    public String getOperationState() {
        return operationState;
    }

    public Integer getTablesInUse() {
        return tablesInUse;
    }

    public Integer getTablesLocked() {
        return tablesLocked;
    }

    public Integer getLockStructs() {
        return lockStructs;
    }

    public Integer getHeapSize() {
        return heapSize;
    }

    public Integer getRowLocks() {
        return rowLocks;
    }

    public TrxLock getWaitingTrxLock() {
        return waitingTrxLock;
    }

    public Set<TrxLock> getHoldingTrxLocks() {
        return holdingTrxLocks == null ? Collections.emptySet() : holdingTrxLocks;
    }

    public boolean isUpdated() {
        return isUpdated;
    }

    // ---------- setter methods ----------

    public void setGroup(String group) {
        this.group = group;
    }

    public void setState(String state) {
        this.state = state;
    }

    public void setPhysicalSql(String physicalSql) {
        this.physicalSql = physicalSql;
    }

    public void setOperationState(String operationState) {
        this.operationState = operationState;
    }

    public void setTablesInUse(Integer tablesInUse) {
        this.tablesInUse = tablesInUse;
    }

    public void setTablesLocked(Integer tablesLocked) {
        this.tablesLocked = tablesLocked;
    }

    public void setLockStructs(Integer lockStructs) {
        this.lockStructs = lockStructs;
    }

    public void setHeapSize(Integer heapSize) {
        this.heapSize = heapSize;
    }

    public void setRowLocks(Integer rowLocks) {
        this.rowLocks = rowLocks;
    }

    public void setWaitingTrxLock(TrxLock trxLock) {
        this.waitingTrxLock = trxLock;
    }

    public void addHoldingTrxLock(TrxLock trxLock) {
        if (null == this.holdingTrxLocks) {
            holdingTrxLocks = new HashSet<>();
        }
        holdingTrxLocks.add(trxLock);
    }

    public void setUpdated(boolean updated) {
        isUpdated = updated;
    }

    @Override
    public int hashCode() {
        // Make the group name case-insensitive
        return group.toLowerCase().hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof LocalTransaction)) {
            return false;
        }
        return group.equalsIgnoreCase(((LocalTransaction) o).group);
    }

    @Override
    public String toString() {
        return "LocalTrx{" +
            "group='" + group + '\'' +
            '}';
    }
}
