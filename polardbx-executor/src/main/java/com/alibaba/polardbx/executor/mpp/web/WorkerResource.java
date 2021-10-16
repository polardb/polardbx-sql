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

package com.alibaba.polardbx.executor.mpp.web;

import com.alibaba.polardbx.gms.node.InternalNode;
import com.alibaba.polardbx.gms.node.InternalNodeManager;
import com.alibaba.polardbx.gms.node.Node;
import com.alibaba.polardbx.gms.node.NodeState;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.util.Set;

import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON_TYPE;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

@Path("/v1/worker")
public class WorkerResource {
    private final InternalNodeManager nodeManager;
    private final HttpClient httpClient;

    @Inject
    public WorkerResource(InternalNodeManager nodeManager, @ForWorkerInfo HttpClient httpClient) {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
    }

    @GET
    @Path("{nodeId}/status")
    public Response getStatus(@PathParam("nodeId") String nodeId) {
        return proxyJsonResponse(nodeId, "v1/status");
    }

    @GET
    @Path("{nodeId}/thread")
    public Response getThreads(@PathParam("nodeId") String nodeId) {
        return proxyJsonResponse(nodeId, "v1/thread");
    }

    private Response proxyJsonResponse(String nodeId, String workerPath) {
        Set<InternalNode> nodes = nodeManager.getNodes(NodeState.ACTIVE, true);
        Node node = nodes.stream()
            .filter(n -> n.getNodeIdentifier().equals(nodeId))
            .findFirst()
            .orElseThrow(() -> new WebApplicationException(NOT_FOUND));

        Request request = prepareGet()
            .setUri(uriBuilderFrom(((InternalNode) node).getHttpUri())
                .appendPath(workerPath)
                .build())
            .build();

        InputStream responseStream = httpClient.execute(request, new StreamingJsonResponseHandler());
        return Response.ok(responseStream, APPLICATION_JSON_TYPE).build();
    }
}
