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

import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.common.constants.SystemTables;
import com.alibaba.polardbx.common.logical.ITConnection;
import com.alibaba.polardbx.common.utils.MasterSlaveUtil;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class LockingFunctionManager {

    static final int MAX_LOCKS_NUMBER = 16;

    static final int EXPIRATION_TIME = 60;

    static final int HEART_BEAT_INTERVAL = 2000;

    private static final LockingFunctionManager INSTANCE = new LockingFunctionManager();

    private Map<String, LockingFunctionHandle> distributedLockHandles;

    private DataSource dataSource;

    private LockingFunctionManager() {
    }

    public void init(DataSource dataSource) {
        this.distributedLockHandles = new HashMap<>();
        this.dataSource = dataSource;
    }

    public static LockingFunctionManager getInstance() {
        return INSTANCE;
    }

    public synchronized LockingFunctionHandle getHandle(ITConnection drdsConnection, long connectionId) {
        return new PolarDBXLockingFunctionHandle(this, drdsConnection, sessionId(connectionId));
    }

    public void removeHandle(String sessionId) {
        distributedLockHandles.remove(sessionId);
    }

    private String sessionId(long connectionId) {
        return TddlNode.getNodeId() + ":" + connectionId;
    }

    Connection getConnection() throws SQLException {
        return MasterSlaveUtil.getMasterConntetion(this.dataSource);
    }

}
