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

package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.server.statistics.utils.SessionUtils;
import com.alibaba.polardbx.common.utils.thread.ServerThreadPool;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.LogicalShowHtcHandler;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.matrix.jdbc.TDataSource;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.stats.MatrixStatistics;

import java.util.List;

/**
 * @author mengshi.sunmengshi 2015年5月12日 下午1:28:16
 * @since 5.1.0
 */
public class ShowStatsSyncAction implements ISyncAction {

    private String db;

    public ShowStatsSyncAction() {
    }

    public ShowStatsSyncAction(String db) {
        this.db = db;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    @Override
    public ResultCursor sync() {
        ArrayResultCursor result = new ArrayResultCursor("RULE");
        result.addColumn("activeConnection", DataTypes.LongType);
        result.addColumn("aggregateMultiDBCount", DataTypes.LongType);
        result.addColumn("connectionCount", DataTypes.LongType);
        result.addColumn("delete", DataTypes.LongType);
        result.addColumn("errorCount", DataTypes.LongType);
        result.addColumn("hintCount", DataTypes.LongType);
        result.addColumn("insert", DataTypes.LongType);
        result.addColumn("integrityConstraintViolationErrorCount", DataTypes.LongType);
        result.addColumn("joinMultiDBCount", DataTypes.LongType);
        result.addColumn("multiDBCount", DataTypes.LongType);
        result.addColumn("netIn", DataTypes.LongType);
        result.addColumn("netOut", DataTypes.LongType);
        result.addColumn("physicalRequest", DataTypes.LongType);
        result.addColumn("physicalTimeCost", DataTypes.LongType);
        result.addColumn("query", DataTypes.LongType);
        result.addColumn("replace", DataTypes.LongType);
        result.addColumn("request", DataTypes.LongType);
        result.addColumn("tempTableCount", DataTypes.LongType);
        result.addColumn("timeCost", DataTypes.LongType);
        result.addColumn("update", DataTypes.LongType);
        result.addColumn("recordTime", DataTypes.LongType);
        result.addColumn("threadRunning", DataTypes.LongType);
        result.addColumn("slowRequest", DataTypes.LongType);
        result.addColumn("physicalSlowRequest", DataTypes.LongType);
        result.addColumn("cpu", DataTypes.DoubleType);
        result.addColumn("freemem", DataTypes.DoubleType);
        result.addColumn("fullgcCount", DataTypes.LongType);
        result.addColumn("fullgcTime", DataTypes.LongType);
        result.addColumn("transCountXA", DataTypes.LongType);
        result.addColumn("transCountBestEffort", DataTypes.LongType);
        result.addColumn("transCountTSO", DataTypes.LongType);
        result.addColumn("backfillRows", DataTypes.LongType);
        result.addColumn("checkedRows", DataTypes.LongType);

        result.initMeta();
        TDataSource ds = CobarServer.getInstance().getConfig().getSchemas().get(db).getDataSource();
        MatrixStatistics stats = ds.getStatistics();

        stats.recordTime = System.currentTimeMillis();

        result.addRow(getRow(stats, ds));
        result.addRow(getRow(stats.getPreviosStatistics(), ds));

        return result;
    }

    private Object[] getRow(MatrixStatistics stats, TDataSource ds) {

        ServerThreadPool exec = CobarServer.getInstance().getServerExecutor();
        List<Object> list = LogicalShowHtcHandler.getHostInfo4Manager(null);
        if (list.size() > 8) {
            return new Object[] {
                SessionUtils.getActiveConnectionsNum(ds.getSchemaName()), stats.aggregateMultiDBCount,
                SessionUtils.getConnectionsNum(ds.getSchemaName()),
                stats.delete, stats.errorCount, stats.hintCount, stats.insert,
                stats.integrityConstraintViolationErrorCount, stats.joinMultiDBCount, stats.multiDBCount, stats.netIn,
                stats.netOut, stats.physicalRequest.get(), stats.physicalTimeCost.get(), stats.query, stats.replace,
                stats.request,
                stats.tempTableCount, stats.timeCost, stats.update, stats.recordTime,
                exec.getTaskCountBySchemaName(ds.getSchemaName()), stats.slowRequest,
                stats.physicalSlowRequest, list.get(1), list.get(3), list.get(7), list.get(8),
                stats.getTransactionStats().countXA.get(),
                stats.getTransactionStats().countBestEffort.get(),
                stats.getTransactionStats().countTSO.get(),
                stats.backfillRows.get(),
                stats.checkedRows.get()};
        } else {
            return new Object[] {
                SessionUtils.getActiveConnectionsNum(ds.getSchemaName()), stats.aggregateMultiDBCount,
                SessionUtils.getConnectionsNum(ds.getSchemaName()),
                stats.delete, stats.errorCount, stats.hintCount, stats.insert,
                stats.integrityConstraintViolationErrorCount, stats.joinMultiDBCount, stats.multiDBCount, stats.netIn,
                stats.netOut, stats.physicalRequest.get(), stats.physicalTimeCost.get(), stats.query, stats.replace,
                stats.request,
                stats.tempTableCount, stats.timeCost, stats.update, stats.recordTime,
                exec.getTaskCountBySchemaName(ds.getSchemaName()), stats.slowRequest,
                stats.physicalSlowRequest, 0D, 0D, 0L, 0L,
                stats.getTransactionStats().countXA.get(),
                stats.getTransactionStats().countBestEffort.get(),
                stats.getTransactionStats().countTSO.get(),
                stats.backfillRows.get(),
                stats.checkedRows.get()};
        }

    }
}
