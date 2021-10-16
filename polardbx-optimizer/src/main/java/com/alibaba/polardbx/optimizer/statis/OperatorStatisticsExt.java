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

package com.alibaba.polardbx.optimizer.statis;

import org.openjdk.jol.info.ClassLayout;

/**
 * Runtime Statistics of a single operator
 */
public class OperatorStatisticsExt extends OperatorStatistics {

    public static long INSTANCE_MEM_SIZE = ClassLayout.parseClass(OperatorStatisticsExt.class).instanceSize();

    // ========= time cost for waiting for getting phy conn =========

    /**
     * Duration of create connection for one phy table operation
     */
    protected long createConnDuration;

    /**
     * Duration of wait to get connection for one phy table operation
     */
    protected long waitConnDuration;

    /**
     * Duration of initialize connection for one phy table operation
     */
    protected long initConnDuration;

    // ========= time cost for execution and waiting for first row =========
    /**
     * Duration of prepare jdbc stmt env for one phy sql
     */
    protected long prepateStmtEnvDuration;

    /**
     * Duration of init jdbc conn env and create stmt for one phy table
     * operation
     */
    protected long createAndInitJdbcStmtDuration;

    /**
     * Duration of exec jdbc stmt for one phy table operation
     */
    protected long execJdbcStmtDuration;

    // ========= time cost for fetching the the whole result set =========

    /**
     * Duration of fetch all JdbcResultSet
     */
    protected long fetchJdbcResultSetDuration;

    /**
     * Duration of close all JdbcResultSet
     */
    protected long closeJdbcResultSetDuration;

    public OperatorStatisticsExt() {
    }

    public long getCreateConnDuration() {
        return createConnDuration;
    }

    public void setCreateConnDuration(long createConnDuration) {
        this.createConnDuration = createConnDuration;
    }

    public long getExecJdbcStmtDuration() {
        return execJdbcStmtDuration;
    }

    public void setExecJdbcStmtDuration(long execJdbcStmtDuration) {
        this.execJdbcStmtDuration = execJdbcStmtDuration;
    }

    public long getWaitConnDuration() {
        return waitConnDuration;
    }

    public void setWaitConnDuration(long waitConnDuration) {
        this.waitConnDuration = waitConnDuration;
    }

    public long getInitConnDuration() {
        return initConnDuration;
    }

    public void setInitConnDuration(long initConnDuration) {
        this.initConnDuration = initConnDuration;
    }

    public long getFetchJdbcResultSetDuration() {
        return fetchJdbcResultSetDuration;
    }

    public void setFetchJdbcResultSetDuration(long fetchJdbcResultSetDuration) {
        this.fetchJdbcResultSetDuration = fetchJdbcResultSetDuration;
    }

    public long getCreateAndInitJdbcStmtDuration() {
        return createAndInitJdbcStmtDuration;
    }

    public void setCreateAndInitJdbcStmtDuration(long createAndInitJdbcStmtDuration) {
        this.createAndInitJdbcStmtDuration = createAndInitJdbcStmtDuration;
    }

    public long getPrepateStmtEnvDuration() {
        return prepateStmtEnvDuration;
    }

    public void setPrepateStmtEnvDuration(long prepateStmtEnvDuration) {
        this.prepateStmtEnvDuration = prepateStmtEnvDuration;
    }

    public long getCloseJdbcResultSetDuration() {
        return closeJdbcResultSetDuration;
    }

    public void setCloseJdbcResultSetDuration(long closeJdbcResultSetDuration) {
        this.closeJdbcResultSetDuration = closeJdbcResultSetDuration;
    }
}
