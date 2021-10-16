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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.mpp.execution.QueryInfo;
import com.alibaba.polardbx.executor.mpp.server.StatementResource;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.calcite.rel.RelNode;

import java.net.URI;
import java.util.concurrent.Future;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_DATA_OUTPUT;
import static java.util.Objects.requireNonNull;

public class LocalStatementClient extends AbstractStatementClient {

    private static final Logger log = LoggerFactory.getLogger(LocalStatementClient.class);

    private final StatementResource statementResource;
    private StatementResource.Query query;

    private URI serverUri;

    public LocalStatementClient(ExecutionContext executionContext, RelNode node,
                                StatementResource statementResource,
                                URI serverUri) {
        super(executionContext, node);
        this.statementResource = requireNonNull(statementResource, "statementResource is null");
        this.serverUri = serverUri;
    }

    @Override
    public void createQuery() throws Exception {
        try {
            LocalResultResponse results = statementResource.createQuery(clientContext, plan, serverUri);
            if (results == null) {
                log.error(mppQueryId + " starting query fail, query:" + query);
                throw new TddlRuntimeException(ERR_DATA_OUTPUT, "starting query fail, query:" + query);
            }
            if (results.getException() != null) {
                if (results.getException() instanceof TddlRuntimeException) {
                    throw results.getException();
                } else {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTE_MPP, results.getException(),
                        "create query error");
                }
            }
            processResponse(results);
            query = statementResource.getQuery(mppQueryId);
            if (query == null && currentResults.get().getError() == null) {
                throw new Exception("queryId:" + mppQueryId + " not found");
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    @Override
    public void deleteQuery() {
        if (mppQueryId != null) {
            statementResource.cancelQueryLocal(mppQueryId);
        }
    }

    @Override
    public void failQuery(Throwable t) {
        if (mppQueryId != null) {
            statementResource.failQueryLocal(mppQueryId, t);
        }
    }

    @Override
    public Object getResults(URI nextUri) {
        LocalResultRequest resultRequest = new LocalResultRequest();
        resultRequest.setToken(nextToken);
        resultRequest.setQueryId(mppQueryId);
        resultRequest.setUriInfo(nextUri.toString());
        return statementResource.getResultLocal(resultRequest);
    }

    public Future<QueryInfo> getBlockedQueryInfo() throws Exception {
        return query.getBlockedQueryInfo();
    }

    public QueryInfo tryGetQueryInfo() throws Exception {
        return query.tryGetQueryInfo();
    }
}
