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

package org.apache.calcite.util.trace;

/**
 * Sketch of runtime statistics to display
 *
 * @author chenghui.lch
 */
public class RuntimeStatisticsSketchExt extends RuntimeStatisticsSketch {

    // ========= time cost from async task of operator or its subOperator
    protected long childrenAsyncTaskDuration = 0;

    protected long selfAsyncTaskDuration = 0;

    /**
     * Total start-up duration in nano
     */
    protected long startupDurationNano = 0;

    /**
     * Total duration in nano
     */
    protected long durationNano = 0;

    /**
     * Total close duration in nano
     */
    protected long closeDurationNano = 0;

    /**
     * Total duration of worker threads run by this operator, unit: nano
     */
    protected long workerDurationNano = 0;

    /**
     * the count of all sub operator statistics
     */
    protected long subOperatorStatCount = 0;

    /**
     * TimeUnit: nano Duration sum of queuing of one phy table operation to be
     * exec, this process do not cost cpu, but affect the response time of sql
     */
    protected long queuingDurationNanoSum = 0;

    /**
     * TimeUnit: nano Duration sum of create connection for one phy table
     * operation
     */
    protected long createConnDurationNanoSum = 0;

    /**
     * TimeUnit: nano Duration sum of wait to get connection for one phy table
     * operation
     */
    protected long waitConnDurationNanoSum = 0;

    /**
     * TimeUnit: nano Duration sum of initialize connection for one phy table
     * operation
     */
    protected long initConnDurationNanoSum = 0;

    /**
     * TimeUnit: nano Total duration sum of get jdbc connection for one phy
     * table operation
     */
    protected long totalGetConnDurationNanoSum = 0;

    /**
     * TimeUnit: nano, Duration sum of create and init jdbc stmt for one phy
     * table operation
     */
    protected long createAndInitJdbcStmtDurationNanoSum = 0;

    /**
     * TimeUnit: nano Duration sum of exec jdbc stmt for one phy table operation
     */
    protected long execJdbcStmtDurationNanoSum = 0;

    /**
     * TimeUnit: nano Duration sum of fetch jdbc stmt for one phy table
     * operation
     */
    protected long fetchJdbcResultSetDurationNanoSum = 0;

    /**
     * TimeUnit: nano Duration sum of close jdbc stmt for one phy table
     * operation
     */
    protected long closeJdbcResultSetDurationNanoSum = 0;

    /**
     * TimeUnit: nano Duration max of queuing of one phy table operation to be
     * exec, this process do not cost cpu, but affect the response time of sql
     */
    protected long queuingDurationNanoMax = 0;

    /**
     * TimeUnit: nano Duration max of create connection for one phy table
     * operation
     */
    protected long createConnDurationNanoMax = 0;

    /**
     * TimeUnit: nano Duration max of wait to get connection for one phy table
     * operation
     */
    protected long waitConnDurationNanoMax = 0;

    /**
     * TimeUnit: nano Duration max of initialize connection for one phy table
     * operation
     */
    protected long initConnDurationNanoMax = 0;

    /**
     * TimeUnit: nano Total duration max of get jdbc connection for one phy
     * table operation
     */
    protected long totalGetConnDurationNanoMax = 0;

    /**
     * TimeUnit: nano Duration max of create and init jdbc stmt for one phy
     * table operation
     */
    protected long createAndInitJdbcStmtDurationNanoMax;

    /**
     * TimeUnit: nano Duration max of exec jdbc stmt for one phy table operation
     */
    protected long execJdbcStmtDurationNanoMax = 0;

    /**
     * TimeUnit: nano Duration max of log jdbc stmt for one phy table operation
     */
    protected long logJdbcStmtDurationNanoMax = 0;

    /**
     * TimeUnit: nano, Duration max of log jdbc stmt for one phy table operation
     */
    protected long fetchJdbcResultSetDurationNanoMax = 0;

    protected boolean hasInputOperator;

    public RuntimeStatisticsSketchExt(long startupDurationNano, long durationNano, long closeDurationNano,
                                      long workerDurationNano, long rowCount, long runtimeFilteredCount, long outputBytes, long memory,
                                      int instances, boolean hasInputOperator, int spillCnt) {

        super((double) startupDurationNano / 1e9,
            (double) durationNano / 1e9,
            (double) workerDurationNano / 1e9,
            rowCount, runtimeFilteredCount,
            outputBytes,
            memory,
            instances,
            spillCnt);
        this.startupDurationNano = startupDurationNano;
        this.durationNano = durationNano;
        this.closeDurationNano = closeDurationNano;
        this.workerDurationNano = workerDurationNano;
        this.hasInputOperator = hasInputOperator;
    }

    public long getStartupDurationNano() {
        return startupDurationNano;
    }

    public void setStartupDurationNano(long startupDurationNano) {
        this.startupDurationNano = startupDurationNano;
    }

    public long getDurationNano() {
        return durationNano;
    }

    public void setDurationNano(long durationNano) {
        this.durationNano = durationNano;
    }

    public long getQueuingDurationNanoMax() {
        return queuingDurationNanoMax;
    }

    public void setQueuingDurationNanoMax(long queuingDurationNanoMax) {
        this.queuingDurationNanoMax = queuingDurationNanoMax;
    }

    public long getCreateConnDurationNanoMax() {
        return createConnDurationNanoMax;
    }

    public void setCreateConnDurationNanoMax(long createConnDurationNanoMax) {
        this.createConnDurationNanoMax = createConnDurationNanoMax;
    }

    public long getWaitConnDurationNanoMax() {
        return waitConnDurationNanoMax;
    }

    public void setWaitConnDurationNanoMax(long waitConnDurationNanoMax) {
        this.waitConnDurationNanoMax = waitConnDurationNanoMax;
    }

    public long getInitConnDurationNanoMax() {
        return initConnDurationNanoMax;
    }

    public void setInitConnDurationNanoMax(long initConnDurationNanoMax) {
        this.initConnDurationNanoMax = initConnDurationNanoMax;
    }

    public long getTotalGetConnDurationNanoMax() {
        return totalGetConnDurationNanoMax;
    }

    public void setTotalGetConnDurationNanoMax(long totalGetConnDurationNanoMax) {
        this.totalGetConnDurationNanoMax = totalGetConnDurationNanoMax;
    }

    public long getExecJdbcStmtDurationNanoMax() {
        return execJdbcStmtDurationNanoMax;
    }

    public void setExecJdbcStmtDurationNanoMax(long execJdbcStmtDurationNanoMax) {
        this.execJdbcStmtDurationNanoMax = execJdbcStmtDurationNanoMax;
    }

    public long getLogJdbcStmtDurationNanoMax() {
        return logJdbcStmtDurationNanoMax;
    }

    public void setLogJdbcStmtDurationNanoMax(long logJdbcStmtDurationNanoMax) {
        this.logJdbcStmtDurationNanoMax = logJdbcStmtDurationNanoMax;
    }

    public long getSubOperatorStatCount() {
        return subOperatorStatCount;
    }

    public void setSubOperatorStatCount(long subOperatorStatCount) {
        this.subOperatorStatCount = subOperatorStatCount;
    }

    public long getQueuingDurationNanoSum() {
        return queuingDurationNanoSum;
    }

    public void setQueuingDurationNanoSum(long queuingDurationNanoSum) {
        this.queuingDurationNanoSum = queuingDurationNanoSum;
    }

    public long getCreateConnDurationNanoSum() {
        return createConnDurationNanoSum;
    }

    public void setCreateConnDurationNanoSum(long createConnDurationNanoSum) {
        this.createConnDurationNanoSum = createConnDurationNanoSum;
    }

    public long getWaitConnDurationNanoSum() {
        return waitConnDurationNanoSum;
    }

    public void setWaitConnDurationNanoSum(long waitConnDurationNanoSum) {
        this.waitConnDurationNanoSum = waitConnDurationNanoSum;
    }

    public long getInitConnDurationNanoSum() {
        return initConnDurationNanoSum;
    }

    public void setInitConnDurationNanoSum(long initConnDurationNanoSum) {
        this.initConnDurationNanoSum = initConnDurationNanoSum;
    }

    public long getTotalGetConnDurationNanoSum() {
        return totalGetConnDurationNanoSum;
    }

    public void setTotalGetConnDurationNanoSum(long totalGetConnDurationNanoSum) {
        this.totalGetConnDurationNanoSum = totalGetConnDurationNanoSum;
    }

    public long getExecJdbcStmtDurationNanoSum() {
        return execJdbcStmtDurationNanoSum;
    }

    public void setExecJdbcStmtDurationNanoSum(long execJdbcStmtDurationNanoSum) {
        this.execJdbcStmtDurationNanoSum = execJdbcStmtDurationNanoSum;
    }

    public long getFetchJdbcResultSetDurationNanoSum() {
        return fetchJdbcResultSetDurationNanoSum;
    }

    public void setFetchJdbcResultSetDurationNanoSum(long fetchJdbcResultSetDurationNanoSum) {
        this.fetchJdbcResultSetDurationNanoSum = fetchJdbcResultSetDurationNanoSum;
    }

    public long getFetchJdbcResultSetDurationNanoMax() {
        return fetchJdbcResultSetDurationNanoMax;
    }

    public void setFetchJdbcResultSetDurationNanoMax(long fetchJdbcResultSetDurationNanoMax) {
        this.fetchJdbcResultSetDurationNanoMax = fetchJdbcResultSetDurationNanoMax;
    }

    public long getCreateAndInitJdbcStmtDurationNanoSum() {
        return createAndInitJdbcStmtDurationNanoSum;
    }

    public void setCreateAndInitJdbcStmtDurationNanoSum(long createAndInitJdbcStmtDurationNanoSum) {
        this.createAndInitJdbcStmtDurationNanoSum = createAndInitJdbcStmtDurationNanoSum;
    }

    public long getCreateAndInitJdbcStmtDurationNanoMax() {
        return createAndInitJdbcStmtDurationNanoMax;
    }

    public void setCreateAndInitJdbcStmtDurationNanoMax(long createAndInitJdbcStmtDurationNanoMax) {
        this.createAndInitJdbcStmtDurationNanoMax = createAndInitJdbcStmtDurationNanoMax;
    }

    public long getCloseDurationNano() {
        return closeDurationNano;
    }

    public void setCloseDurationNano(long closeDurationNano) {
        this.closeDurationNano = closeDurationNano;
    }

    public long getChildrenAsyncTaskDuration() {
        return childrenAsyncTaskDuration;
    }

    public void setChildrenAsyncTaskDuration(long childrenAsyncTaskDuration) {
        this.childrenAsyncTaskDuration = childrenAsyncTaskDuration;
    }

    public long getSelfAsyncTaskDuration() {
        return selfAsyncTaskDuration;
    }

    public void setSelfAsyncTaskDuration(long selfAsyncTaskDuration) {
        this.selfAsyncTaskDuration = selfAsyncTaskDuration;
    }

    public long getCloseJdbcResultSetDurationNanoSum() {
        return closeJdbcResultSetDurationNanoSum;
    }

    public void setCloseJdbcResultSetDurationNanoSum(long closeJdbcResultSetDurationNanoSum) {
        this.closeJdbcResultSetDurationNanoSum = closeJdbcResultSetDurationNanoSum;
    }

    public boolean hasInputOperator() {
        return hasInputOperator;
    }
}
