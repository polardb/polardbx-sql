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

package com.alibaba.polardbx.transaction.log;

import com.alibaba.polardbx.transaction.TransactionType;
import com.alibaba.polardbx.transaction.TransactionState;

public class GlobalTxLog {

    private long txid;
    private String group; // primary group
    private TransactionState state;
    private TransactionType type;
    private String serverAddr;
    private int retires;
    private String error;
    private ConnectionContext context;
    private Long commitTimestamp;

    public long getTxid() {
        return txid;
    }

    public void setTxid(long txid) {
        this.txid = txid;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public TransactionState getState() {
        return state;
    }

    public TransactionType getType() {
        return type;
    }

    public void setType(TransactionType type) {
        this.type = type;
    }

    public void setState(TransactionState state) {
        this.state = state;
    }

    @Deprecated
    public int getRetires() {
        return retires;
    }

    @Deprecated
    public void setRetires(int retires) {
        this.retires = retires;
    }

    public String getServerAddr() {
        return serverAddr;
    }

    public void setServerAddr(String serverAddr) {
        this.serverAddr = serverAddr;
    }

    @Deprecated
    public String getError() {
        return error;
    }

    @Deprecated
    public void setError(String error) {
        this.error = error;
    }

    public ConnectionContext getContext() {
        return context;
    }

    public void setContext(ConnectionContext context) {
        this.context = context;
    }

    public Long getCommitTimestamp() {
        return commitTimestamp;
    }

    public void setCommitTimestamp(Long commitTimestamp) {
        this.commitTimestamp = commitTimestamp;
    }
}
