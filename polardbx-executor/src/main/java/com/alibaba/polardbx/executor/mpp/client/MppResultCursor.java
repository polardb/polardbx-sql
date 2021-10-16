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

package com.alibaba.polardbx.executor.mpp.client;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.executor.cursor.AbstractCursor;
import com.alibaba.polardbx.executor.mpp.execution.QueryInfo;
import com.alibaba.polardbx.executor.mpp.operator.OperatorStats;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.statistics.RuntimeStatistics;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;

import static com.alibaba.polardbx.executor.mpp.server.StatementResource.Query.toQueryError;

public class MppResultCursor extends AbstractCursor {

    private static final Logger log = LoggerFactory.getLogger(MppResultCursor.class);
    private final LocalStatementClient client;
    private final ExecutionContext ec;
    private Row currentValue;
    private Iterator<Object> currentChunks;
    private Chunk currentChunk;
    private int nextPos;
    private boolean bWaitQueryInfo;
    private CursorMeta cursorMeta;

    public MppResultCursor(LocalStatementClient client, ExecutionContext executionContext, CursorMeta cursorMeta) {
        super(false);
        this.client = client;
        this.ec = executionContext;
        this.cursorMeta = cursorMeta;
    }

    @Override
    public Row doNext() {
        if (currentChunk == null || currentChunk.getPositionCount() == nextPos) {
            if (currentChunks == null || !currentChunks.hasNext()) {
                //has next
                while (client.isValid()) {
                    QueryResults clientResults = client.current();
                    QueryError queryError = clientResults.getError();
                    if (queryError != null) {
                        log.error("MppError:" + ec.getTraceId() + ",SchemaName=" + ec.getSchemaName() + ",stackInfo="
                            + queryError.getFailureInfo());
                        throw GeneralUtil.nestedException(queryError.toException());
                    }
                    Iterable<Object> data = clientResults.getData();
                    if (data != null) {
                        currentChunks = data.iterator();
                        client.advance();
                        break;
                    }
                    client.advance();
                }
            }

            if (currentChunks != null && currentChunks.hasNext()) {
                currentChunk = (Chunk) currentChunks.next();
                nextPos = 0;
            }
        }

        if (currentChunk != null && currentChunk.getPositionCount() > nextPos) {
            currentValue = currentChunk.rowAt(nextPos++);
            currentValue.setCursorMeta(cursorMeta);
        } else {
            currentValue = null;
        }
        return currentValue;
    }

    @Override
    public List<Throwable> doClose(List<Throwable> exceptions) {
        if (exceptions == null || exceptions.size() == 0) {
            if (ec.getRuntimeStatistics() != null && bWaitQueryInfo) {
                //analyze the sql
                try {
                    QueryInfo queryInfo = getQueryInfo();
                    if (queryInfo != null) {
                        QueryError queryError = toQueryError(queryInfo);
                        if (queryError != null) {
                            FailureInfo failureInfo = queryError.getFailureInfo();
                            if (failureInfo != null) {
                                throw GeneralUtil.nestedException(failureInfo.toExceptionWithoutType());
                            }
                        }
                        List<OperatorStats> operatorStats = queryInfo.getQueryStats().getOperatorSummaries();
                        for (OperatorStats operatorStat : operatorStats) {

                            RuntimeStatistics runtimeStat = (RuntimeStatistics) ec.getRuntimeStatistics();
                            runtimeStat.collectMppStats(operatorStat.getOperatorId(),
                                operatorStat.toSketch());
                        }
                    }
                } catch (Exception e) {
                    exceptions.add(e);
                }
            }
        }
        if (exceptions != null && exceptions.size() > 0) {
            client.failQuery(exceptions.get(0));
        } else {
            client.close();
        }
        if (exceptions == null) {
            exceptions = new ArrayList();
        }
        return exceptions;
    }

    public QueryInfo getQueryInfo() throws Exception {
        if (bWaitQueryInfo) {
            Future<QueryInfo> blockedQueryInfo = client.getBlockedQueryInfo();
            while (!blockedQueryInfo.isDone()) {
                Thread.sleep(100);
                client.tryGetQueryInfo();
            }
            return blockedQueryInfo.get();
        } else {
            //the return queryInfo maybe null, or it is not the final queryInfo
            return client.tryGetQueryInfo();
        }
    }

    public void waitQueryInfo(boolean bWaitQueryInfo) {
        this.bWaitQueryInfo = bWaitQueryInfo;
    }
}
