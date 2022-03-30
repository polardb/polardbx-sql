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

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.executor.mpp.web;

import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.mpp.execution.QueryExecution;
import com.alibaba.polardbx.executor.mpp.execution.QueryInfo;
import com.alibaba.polardbx.executor.mpp.execution.QueryManager;
import com.alibaba.polardbx.executor.mpp.execution.QueryState;
import com.alibaba.polardbx.executor.mpp.execution.StageId;
import com.alibaba.polardbx.executor.mpp.server.BasicQueryInfo;
import com.alibaba.polardbx.common.utils.bloomfilter.BloomFilterInfo;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.NoSuchElementException;

import static com.google.common.base.Strings.isNullOrEmpty;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;

/**
 * Manage queries scheduled on this node
 */
@Path("/v1/query")
public class QueryResource {
    private static final Logger logger = LoggerFactory.getLogger(QueryResource.class);
    private final QueryManager queryManager;
    private final HttpClient httpClient;

    @Inject
    public QueryResource(QueryManager queryManager, @ForQueryInfo HttpClient httpClient) {
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.httpClient = httpClient;
    }

    @GET
    public List<BasicQueryInfo> getAllQueryInfo() {
        List<BasicQueryInfo> ret = extractBasicQueryInfo(queryManager.getAllQueryInfo());
        return ret;
    }

    private static List<BasicQueryInfo> extractBasicQueryInfo(List<QueryInfo> allQueryInfo) {
        ImmutableList.Builder<BasicQueryInfo> basicQueryInfo = ImmutableList.builder();
        for (QueryInfo queryInfo : allQueryInfo) {
            basicQueryInfo.add(new BasicQueryInfo(queryInfo));
        }
        return basicQueryInfo.build();
    }

    @GET
    @Path("proxy/{proxyUrl}")
    public Response getProxyQueryInfo(@PathParam("proxyUrl") String proxyUrl) {
        return proxyJsonResponse(proxyUrl);
    }

    private Response proxyJsonResponse(String proxyUrl) {

        Request request = prepareGet()
            .setUri(URI.create(proxyUrl))
            .build();
        InputStream responseStream = httpClient.execute(request, new StreamingJsonResponseHandler());
        return Response.ok(responseStream, APPLICATION_JSON_TYPE).build();
    }

    @GET
    @Path("{queryId}")
    public Response getQueryInfo(@PathParam("queryId") String queryId) {
        requireNonNull(queryId, "queryId is null");

        try {
            QueryInfo queryInfo = queryManager.getQueryInfo(queryId);
            QueryInfo summary = queryInfo;
            try {
                summary = queryInfo.summary();
            } catch (Exception e) {
                // ignore exceptions
            }
            return Response.ok(summary).build();
        } catch (NoSuchElementException e) {
            return Response.status(Status.GONE).build();
        }
    }

    @DELETE
    @Path("{queryId}")
    public void cancelQuery(@PathParam("queryId") String queryId) {
        requireNonNull(queryId, "queryId is null");
        queryManager.cancelQuery(queryId);
    }

    @PUT
    @Path("{queryId}/killed")
    public Response killQuery(@PathParam("queryId") String queryId, String message) {
        TddlRuntimeException exception = new TddlRuntimeException(ErrorCode.ERR_KILLED_QUERY,
            (isNullOrEmpty(message) ? "No message provided." : "Message: " + message));
        return failQuery(queryId, exception);
    }

    private Response failQuery(String queryId, TddlRuntimeException queryException) {
        requireNonNull(queryId, "queryId is null");

        try {
            QueryState state = queryManager.getQueryState(queryId);

            // check before killing to provide the proper error code (this is racy)
            if (state == null || state.isDone()) {
                return Response.status(Status.CONFLICT).build();
            }

            queryManager.failQuery(queryId, queryException);

            // verify if the query was failed (if not, we lost the race)
            if (!queryException.getErrorCodeType().equals(queryManager.getQueryInfo(queryId).getErrorCode())) {
                return Response.status(Status.CONFLICT).build();
            }

            return Response.status(Status.OK).build();
        } catch (NoSuchElementException e) {
            return Response.status(Status.GONE).build();
        }
    }

    @DELETE
    @Path("stage/{stageId}")
    public void cancelStage(@PathParam("stageId") StageId stageId) {
        requireNonNull(stageId, "stageId is null");
        queryManager.cancelStage(stageId);
    }

    @POST
    @Path("{queryId}/runtimefilter")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response postNodeInfo(
        @PathParam("queryId") String queryId,
        List<BloomFilterInfo> filterInfos) {

        try {
            QueryExecution queryExecution = queryManager.getQueryExecution(queryId);
            if (queryExecution != null) {
                queryExecution.mergeBloomFilter(filterInfos);
            }
            return Response.ok().build();
        } catch (Exception e) {
            logger.error(
                String.format("Failed to update bloom filter for query: %s, filter infos: %s", queryId, filterInfos),
                e);
            return Response.status(Status.INTERNAL_SERVER_ERROR.getStatusCode(), e.getMessage())
                .build();
        }

    }
}
