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
package com.alibaba.polardbx.executor.mpp.server.remotetask;

import com.alibaba.polardbx.executor.mpp.execution.LocationFactory;
import com.alibaba.polardbx.executor.mpp.execution.StageId;
import com.alibaba.polardbx.executor.mpp.execution.TaskId;
import com.alibaba.polardbx.executor.mpp.metadata.TaskLocation;
import com.alibaba.polardbx.gms.node.InternalNode;
import com.alibaba.polardbx.gms.node.Node;
import io.airlift.http.server.HttpServerInfo;

import javax.inject.Inject;
import java.net.URI;

import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static java.util.Objects.requireNonNull;

public class HttpLocationFactory implements LocationFactory {

    private final Node node;
    private final String baseUriPath;

    @Inject
    public HttpLocationFactory(InternalNode currentNode, HttpServerInfo httpServerInfo) {
        this.node = currentNode;
        this.baseUriPath = uriBuilderFrom(httpServerInfo.getHttpUri()).toString();
    }

    @Override
    public URI createQueryLocation(String queryId) {
        requireNonNull(queryId, "queryId is null");
        return URI.create(baseUriPath + "/v1/query/" + queryId);
    }

    @Override
    public URI createRuntimeFilterLocation(String queryId) {
        requireNonNull(queryId, "queryId is null");
        return URI.create(baseUriPath + "/v1/query/" + queryId + "/runtimefilter");
    }

    @Override
    public URI createDdlFilterLocation(String jobId) {
        requireNonNull(jobId, "jobId is null");
        return URI.create(baseUriPath + "/v1/ddl/" + jobId);
    }

    @Override
    public URI createStageLocation(StageId stageId) {
        requireNonNull(stageId, "stageId is null");
        return URI.create(baseUriPath + "/v1/stage/" + stageId.toString());
    }

    @Override
    public TaskLocation createLocalTaskLocation(TaskId taskId) {
        TaskLocation location = new TaskLocation(node.getNodeServer(), taskId);
        return location;
    }

    @Override
    public TaskLocation createTaskLocation(Node node, TaskId taskId) {
        requireNonNull(node, "node is null");
        requireNonNull(taskId, "taskId is null");
        return new TaskLocation(node.getNodeServer(), taskId);
    }
}
