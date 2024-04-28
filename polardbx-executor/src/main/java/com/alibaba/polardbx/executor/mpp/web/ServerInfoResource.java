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

import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.gms.node.NodeState;
import com.alibaba.polardbx.gms.node.NodeVersion;
import io.airlift.node.NodeInfo;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Optional;

import static io.airlift.units.Duration.nanosSince;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;

@Path("/v1/info")
public class ServerInfoResource {
    private final NodeVersion version;
    private final String environment;
    private final long startTime = System.nanoTime();
    private final String workerId;

    @Inject
    public ServerInfoResource(
        NodeVersion nodeVersion, NodeInfo nodeInfo) {
        this.version = requireNonNull(nodeVersion, "nodeVersion is null");
        this.environment = requireNonNull(nodeInfo, "nodeInfo is null").getEnvironment();
        this.workerId = String.format("%s_%s", TddlNode.getInstId(), TddlNode.getNodeId());
    }

    @GET
    @Produces(APPLICATION_JSON)
    public ServerInfo getInfo() {
        return new ServerInfo(
            version, environment, ServiceProvider.getInstance().getServer().isCoordinator(),
            Optional.of(nanosSince(startTime)), workerId);
    }

    @PUT
    @Path("state")
    @Consumes(APPLICATION_JSON)
    @Produces(TEXT_PLAIN)
    public Response updateState(NodeState state)
        throws WebApplicationException {
        requireNonNull(state, "state is null");
        switch (state) {
        case ACTIVE:
        case INACTIVE:
            throw new WebApplicationException(Response
                .status(BAD_REQUEST)
                .type(MediaType.TEXT_PLAIN)
                .entity(format("Invalid state transition to %s", state))
                .build());
        default:
            return Response.status(BAD_REQUEST)
                .type(TEXT_PLAIN)
                .entity(format("Invalid state %s", state))
                .build();
        }
    }

    @GET
    @Path("state")
    @Produces(APPLICATION_JSON)
    public NodeState getServerState() {
        return NodeState.ACTIVE;
    }

    @GET
    @Path("coordinator")
    @Produces(TEXT_PLAIN)
    public Response getServerCoordinator() {
        if (ServiceProvider.getInstance().getServer().isCoordinator()) {
            return Response.ok().build();
        }
        // return 404 to allow load balancers to only send traffic to the coordinator
        return Response.status(Response.Status.NOT_FOUND).build();
    }
}