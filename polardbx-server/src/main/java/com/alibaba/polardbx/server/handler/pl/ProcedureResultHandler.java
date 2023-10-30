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

package com.alibaba.polardbx.server.handler.pl;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.executor.pl.PLUtils;
import com.alibaba.polardbx.executor.pl.PlCacheCursor;
import com.alibaba.polardbx.executor.pl.PlContext;
import com.alibaba.polardbx.matrix.jdbc.TResultSet;
import com.alibaba.polardbx.net.compress.IPacketOutputProxy;
import com.alibaba.polardbx.net.packet.MySQLPacket;
import com.alibaba.polardbx.net.packet.OkPacket;
import com.alibaba.polardbx.server.QueryResultHandler;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.executor.utils.ResultSetUtil;

import java.sql.ResultSet;
import java.util.concurrent.atomic.AtomicLong;

public class ProcedureResultHandler implements QueryResultHandler {
    private static final Logger logger = LoggerFactory.getLogger(ProcedureResultHandler.class);

    IPacketOutputProxy proxy;

    ServerConnection serverConnection;

    PlContext plContext;

    protected long affectRows = 0;

    protected long lastAffectRows = 0;

    protected long lastFoundRows = 0;

    /**
     * Execute select statement in order to get its result, rather than write back to client
     **/
    private boolean selectForDeeperUse = false;

    /**
     * Execution result of select under selectForDeeperUse mode
     **/
    private PlCacheCursor cursor = null;

    /**
     * Record the exception
     **/
    private Throwable exception = null;

    private boolean hasMore;

    public ProcedureResultHandler(IPacketOutputProxy proxy, ServerConnection serverConnection, PlContext plContext,
                                  boolean hasMore) {
        this.proxy = proxy;
        this.serverConnection = serverConnection;
        this.plContext = plContext;
        this.hasMore = hasMore;
        proxy.packetBegin();
    }

    @Override
    public void sendUpdateResult(long affRows) {
        lastFoundRows = 0;
        lastAffectRows = affRows;
        affectRows += affRows;
    }

    @Override
    public void sendSelectResult(ResultSet resultSet, AtomicLong outAffectedRows, long sqlSelectLimit)
        throws Exception {
        if (!selectForDeeperUse) {
            lastAffectRows = 0;
            proxy =
                ResultSetUtil.resultSetToPacket(resultSet, serverConnection.getCharset(), serverConnection,
                    outAffectedRows,
                    proxy, sqlSelectLimit);
            lastFoundRows = outAffectedRows.get();
        } else {
            cursor = PLUtils.buildCacheCursor(((TResultSet) resultSet).getResultCursor(), plContext);
        }
    }

    @Override
    public void sendPacketEnd(boolean hasMoreResults) {
        // do nothing
    }

    @Override
    public void handleError(Throwable ex, ByteString sql, boolean fatal) {
        logger.error("execute sql under procedure failed, sql: " + sql);
        exception = ex;
    }

    public void writeAffectRows() {
        // TODO check other status
        OkPacket ok = new OkPacket();
        ok.packetId = serverConnection.getNewPacketId();
        ok.affectedRows = affectRows;
        if (serverConnection.isAutocommit()) {
            ok.serverStatus = MySQLPacket.SERVER_STATUS_AUTOCOMMIT;
        } else {
            ok.serverStatus = MySQLPacket.SERVER_STATUS_IN_TRANS;
        }
        if (hasMore) {
            ok.serverStatus |= MySQLPacket.SERVER_MORE_RESULTS_EXISTS;
        }
        ok.write(proxy);
    }

    public void writeBackToClient() {
        proxy.packetEnd();
    }

    public void setSelectForDeeperUse(boolean selectForDeeperUse) {
        this.selectForDeeperUse = selectForDeeperUse;
    }

    public Throwable getException() {
        return exception;
    }

    public void clearException() {
        exception = null;
    }

    public PlCacheCursor getCurosr() {
        return cursor;
    }

    public IPacketOutputProxy getProxy() {
        return proxy;
    }
}
